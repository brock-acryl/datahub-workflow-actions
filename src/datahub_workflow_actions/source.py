"""Ingestion-recipe wrapper so the same engine runs from the recipe the MFE
writes (``source.type: datahub-workflow-actions``) on a local or remote
executor. It starts a datahub-actions pipeline in-process: a Kafka
EntityChangeEvent source (connection under ``source.config.kafka``, defaulting
to the executor's usual env) feeding ``WorkflowActionsAction`` with the rules
block. Runs until the process is stopped; emits no metadata itself."""

from __future__ import annotations

import os

import logging
from typing import Literal, Any, Dict, Iterable, Optional

from pydantic import BaseModel, ConfigDict, Field

from datahub_workflow_actions.contract import RulesConfig, load_rules

logger = logging.getLogger("datahub_workflow_actions.source")

try:
    from datahub.configuration.common import ConfigModel
    from datahub.ingestion.api.common import PipelineContext as IngestionPipelineContext
    from datahub.ingestion.api.source import Source, SourceReport
    from datahub.ingestion.api.workunit import MetadataWorkUnit
except ImportError:  # pragma: no cover
    Source = object  # type: ignore[misc,assignment]
    SourceReport = object  # type: ignore[misc,assignment]
    ConfigModel = BaseModel  # type: ignore[misc,assignment]
    IngestionPipelineContext = Any  # type: ignore[misc,assignment]
    MetadataWorkUnit = Any  # type: ignore[misc,assignment]


# datahub-actions' kafka source hard-codes max.poll.interval.ms = 10 s: a rule that takes
# longer is evicted from the consumer group mid-run and redelivered. Our pipeline runs one
# event at a time, so the gap between polls *is* the rule's run time — allow 15 minutes.
MAX_POLL_INTERVAL_MS = str(15 * 60 * 1000)
DEFAULT_PIPELINE_NAME = "workflow-actions"


def consumer_defaults() -> Dict[str, Any]:
    defaults: Dict[str, Any] = {"max.poll.interval.ms": MAX_POLL_INTERVAL_MS}
    protocol = os.environ.get("KAFKA_PROPERTIES_SECURITY_PROTOCOL")
    if protocol:
        defaults["security.protocol"] = protocol
    return defaults


def with_consumer_defaults(kafka: Dict[str, Any]) -> Dict[str, Any]:
    """Apply our consumer defaults underneath whatever the recipe specifies."""
    connection = dict(kafka.get("connection") or {})
    connection["consumer_config"] = {**consumer_defaults(), **(connection.get("consumer_config") or {})}
    return {**kafka, "connection": connection}


def default_kafka_config() -> Dict[str, Any]:
    """datahub-actions' kafka source defaults to localhost:9092; inside an executor
    the broker and schema registry come from the environment instead."""
    connection: Dict[str, Any] = {
        "bootstrap": os.environ.get("KAFKA_BOOTSTRAP_SERVER", "localhost:9092"),
        "schema_registry_url": os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:8081"),
    }
    return with_consumer_defaults({"connection": connection})


def executor_id_from_env() -> str:
    return os.environ.get("DATAHUB_EXECUTOR_WORKER_ID") or os.environ.get("DATAHUB_EXECUTOR_ID") or "default"


def graph_config_from_env() -> Optional[Dict[str, Any]]:
    """DataHub client config from the executor / CLI environment (DATAHUB_GMS_URL or
    DATAHUB_GMS_HOST/PORT/PROTOCOL, DATAHUB_GMS_TOKEN)."""
    server = os.environ.get("DATAHUB_GMS_URL")
    if not server and os.environ.get("DATAHUB_GMS_HOST"):
        protocol = os.environ.get("DATAHUB_GMS_PROTOCOL", "http")
        port = os.environ.get("DATAHUB_GMS_PORT")
        server = f"{protocol}://{os.environ['DATAHUB_GMS_HOST']}" + (f":{port}" if port else "")
    if not server:
        return None
    token = os.environ.get("DATAHUB_GMS_TOKEN")
    return {"server": server, **({"token": token} if token else {})}


def ensure_visible_logging() -> None:
    """The datahub CLI drops INFO records from packages it doesn't own, so give our
    logger its own stream handler once — rule outcomes must show up in the executor log."""
    ours = logging.getLogger("datahub_workflow_actions")
    if any(getattr(h, "_workflow_actions", False) for h in ours.handlers):
        return
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("[%(asctime)s] %(levelname)-8s {%(name)s} - %(message)s"))
    handler._workflow_actions = True  # type: ignore[attr-defined]
    ours.addHandler(handler)
    ours.setLevel(logging.INFO)
    ours.propagate = False


class WorkflowActionsSourceConfig(ConfigModel):  # type: ignore[misc]
    model_config = ConfigDict(extra="allow")

    schemaVersion: int = 1
    rules: list = Field(default_factory=list)
    kafka: Optional[Dict[str, Any]] = Field(None, description="datahub-actions kafka source config (connection, topic_routes).")
    statePath: Optional[str] = None
    dryRun: bool = False
    pipelineName: Optional[str] = Field(
        None,
        description="datahub-actions pipeline name — also the Kafka consumer group. Default: workflow-actions-<executorId>, so each executor's engine gets its own group and sees every event.",
    )
    executorId: Optional[str] = Field(None, description="Executor pool this source runs on (the MFE writes it); falls back to DATAHUB_EXECUTOR_WORKER_ID.")
    eventSource: Literal["auto", "kafka", "datahub-cloud"] = Field(
        "auto",
        description="How the engine listens. kafka: subscribe to DataHub's broker (needs network access to it). datahub-cloud: poll GMS's Events API over HTTPS (works from a remote executor). auto: kafka when a broker is configured (recipe `kafka` block or KAFKA_BOOTSTRAP_SERVER), else datahub-cloud.",
    )
    cloudEvents: Optional[Dict[str, Any]] = Field(
        None, description="Overrides for the datahub-cloud event source (lookback_days, reset_offsets, …)."
    )
    reloadIntervalSeconds: float = Field(
        30.0, ge=0, description="How often the running engine re-reads its recipe from DataHub and hot-swaps rules/connections/templates. 0 disables."
    )
    connections: Optional[Dict[str, Any]] = Field(None, description="Connections for `sql` steps: {name: url | {url} | {ingestionSource: urn} | {fromEntity: true}}. URLs may use ${SECRET} placeholders.")
    sqlTemplates: Optional[list] = Field(None, description="Opaque to the action: SQL statement templates managed by the MFE.")
    runHistory: Optional[Dict[str, Any]] = Field(None, description="Run history recorded to DataHub as data-process runs: {enabled: true, recordDryRuns: false}.")
    dedupeWindowSeconds: float = Field(
        30.0, ge=0, description="§21 Treat the same change (entity, category, operation, modifier) seen again within this many seconds as a duplicate. 0 disables."
    )
    limits: Optional[Dict[str, Any]] = Field(None, description="§21 Volume limits: {maxRunsPerRulePerMinute: int}. Unset = unlimited.")


class WorkflowActionsSource(Source):  # type: ignore[misc]
    """Long-running: blocks in get_workunits while the actions pipeline consumes events."""

    def __init__(self, config: WorkflowActionsSourceConfig, ctx: Any):
        super().__init__(ctx)  # type: ignore[misc]
        self.config = config
        self.rules: RulesConfig = load_rules({"schemaVersion": config.schemaVersion, "rules": config.rules})
        self.report = SourceReport()  # type: ignore[call-arg]

    @classmethod
    def create(cls, config_dict: dict, ctx: Any) -> "WorkflowActionsSource":
        return cls(WorkflowActionsSourceConfig.model_validate(config_dict), ctx)

    def datahub_client_config(self) -> Optional[Dict[str, Any]]:
        """The DataHub connection the actions pipeline (and so every step, the
        resolver and the run recorder) should use: the ingestion pipeline's own
        graph when the executor gives us one, else the executor environment."""
        graph = getattr(getattr(self, "ctx", None), "graph", None)
        cfg = getattr(graph, "config", None)
        if cfg is not None:
            out = {
                key: getattr(cfg, key, None)
                for key in ("server", "token", "timeout_sec", "retry_status_codes", "retry_max_times", "extra_headers", "ca_certificate_path", "client_certificate_path", "disable_ssl_verification")
            }
            return {k: v for k, v in out.items() if v not in (None, "", {}, [])}
        return graph_config_from_env()

    def effective_pipeline_name(self) -> str:
        """Kafka consumer group: one per executor, so two engines never split partitions."""
        if self.config.pipelineName:
            return self.config.pipelineName
        return f"{DEFAULT_PIPELINE_NAME}-{self.config.executorId or executor_id_from_env()}"

    def source_urn(self) -> Optional[str]:
        """The ingestion source this recipe came from — the executor passes it as the run's pipeline_name."""
        name = getattr(getattr(self, "ctx", None), "pipeline_name", None)
        return name if isinstance(name, str) and name.startswith("urn:li:dataHubIngestionSource:") else None

    def resolve_event_source(self) -> str:
        """Mirror the executor's own rule: direct Kafka when a broker is reachable, else the Events API."""
        if self.config.eventSource != "auto":
            return self.config.eventSource
        if self.config.kafka or os.environ.get("KAFKA_BOOTSTRAP_SERVER") or os.environ.get("DATAHUB_EXECUTOR_INTERNAL_TOPIC"):
            return "kafka"
        return "datahub-cloud"

    def event_source_config(self) -> Dict[str, Any]:
        if self.resolve_event_source() == "kafka":
            kafka = with_consumer_defaults(self.config.kafka) if self.config.kafka else default_kafka_config()
            return {"type": "kafka", "config": kafka}
        # datahub-actions' DataHubEventSource: polls GMS /openapi/v1/events/poll with the pipeline's
        # graph; offsets are stored server-side under the pipeline name (our per-executor group).
        # It aborts the pipeline if an event isn't acked within event_processing_time_max_duration_seconds
        # (default 60 s) — the Events-API twin of Kafka's poll ceiling — so allow the same 15 minutes.
        return {
            "type": "datahub-cloud",
            "config": {
                "topics": "PlatformEvent_v1",
                "event_processing_time_max_duration_seconds": int(MAX_POLL_INTERVAL_MS) // 1000,
                **(self.config.cloudEvents or {}),
            },
        }

    def actions_pipeline_config(self) -> Dict[str, Any]:
        source = self.event_source_config()
        datahub = self.datahub_client_config()
        if source["type"] == "datahub-cloud" and not datahub:
            logger.warning("workflow-actions: the datahub-cloud event source needs a DataHub connection (none found)")
        return {
            "name": self.effective_pipeline_name(),
            **({"datahub": datahub} if datahub else {}),
            "source": source,
            # §21: every change event reaches the action; its EventIndex rejects non-candidates
            # with one dict lookup, so the (client-side) pipeline filter only narrows the type.
            "filter": {"event_type": "EntityChangeEvent_v1"},
            "action": {
                "type": "workflow_actions",
                "config": {
                    "schemaVersion": self.rules.schemaVersion,
                    "rules": [r.model_dump(exclude_none=True) for r in self.rules.rules],
                    **({"statePath": self.config.statePath} if self.config.statePath else {}),
                    **({"connections": self.config.connections} if self.config.connections else {}),
                    **({"runHistory": self.config.runHistory} if self.config.runHistory else {}),
                    "dedupeWindowSeconds": self.config.dedupeWindowSeconds,
                    **({"limits": self.config.limits} if self.config.limits else {}),
                    "dryRun": self.config.dryRun,
                },
            },
        }

    def get_workunits(self) -> Iterable[Any]:
        from datahub_actions.pipeline.pipeline import Pipeline

        ensure_visible_logging()
        pipeline_config = self.actions_pipeline_config()
        if "datahub" not in pipeline_config:
            logger.warning("workflow-actions: no DataHub connection available — steps will run without writing to DataHub")
        logger.info(
            "workflow-actions: starting actions pipeline with %s rule(s) against %s — listening via %s as %s",
            len(self.rules.rules),
            (pipeline_config.get("datahub") or {}).get("server", "no DataHub"),
            pipeline_config["source"]["type"],
            pipeline_config["name"],
        )
        pipeline = Pipeline.create(pipeline_config)
        watcher = self._start_recipe_watcher(pipeline)
        try:
            pipeline.run()  # blocks until stopped
        finally:
            if watcher is not None:
                watcher.stop()
        return iter(())

    def _start_recipe_watcher(self, pipeline: Any):
        """§18.24 hot reload: re-read this source's recipe from DataHub on a timer and swap the
        rules / connections / SQL templates between events — no restart, nothing in flight is cut."""
        from datahub_workflow_actions.reload import RecipeWatcher, fetch_source_config

        urn = self.source_urn()
        graph = getattr(getattr(self, "ctx", None), "graph", None)
        action = getattr(pipeline, "action", None)
        interval = self.config.reloadIntervalSeconds
        if not urn or graph is None or action is None or not hasattr(action, "apply_config") or interval <= 0:
            logger.info("workflow-actions: hot reload off (%s)", "disabled" if interval <= 0 else "no source urn / graph")
            return None
        watcher = RecipeWatcher(
            fetch=lambda: fetch_source_config(graph, urn),
            apply=action.apply_config,
            interval_seconds=interval,
            initial=self.config.model_dump(exclude_none=True),
        )
        watcher.start()
        logger.info("workflow-actions: hot reload every %ss from %s", interval, urn)
        return watcher

    def get_report(self) -> Any:
        return self.report

    def close(self) -> None:
        pass
