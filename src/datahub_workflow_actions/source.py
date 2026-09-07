"""Ingestion-recipe wrapper so the same engine runs from the recipe the MFE
writes (``source.type: datahub-workflow-actions``) on a local or remote
executor. It starts a datahub-actions pipeline in-process: a Kafka
EntityChangeEvent source (connection under ``source.config.kafka``, defaulting
to the executor's usual env) feeding ``WorkflowActionsAction`` with the rules
block. Runs until the process is stopped; emits no metadata itself."""

from __future__ import annotations

import os

import logging
from typing import Any, Dict, Iterable, Optional

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


def default_kafka_config() -> Dict[str, Any]:
    """datahub-actions' kafka source defaults to localhost:9092; inside an executor
    the broker and schema registry come from the environment instead."""
    connection: Dict[str, Any] = {
        "bootstrap": os.environ.get("KAFKA_BOOTSTRAP_SERVER", "localhost:9092"),
        "schema_registry_url": os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:8081"),
    }
    protocol = os.environ.get("KAFKA_PROPERTIES_SECURITY_PROTOCOL")
    if protocol:
        connection["consumer_config"] = {"security.protocol": protocol}
    return {"connection": connection}


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
    pipelineName: str = "workflow-actions"
    connections: Optional[Dict[str, Any]] = Field(None, description="Connections for `sql` steps: {name: url | {url} | {ingestionSource: urn} | {fromEntity: true}}. URLs may use ${SECRET} placeholders.")
    sqlTemplates: Optional[list] = Field(None, description="Opaque to the action: SQL statement templates managed by the MFE.")
    runHistory: Optional[Dict[str, Any]] = Field(None, description="Run history recorded to DataHub as data-process runs: {enabled: true, recordDryRuns: false}.")


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

    def actions_pipeline_config(self) -> Dict[str, Any]:
        source: Dict[str, Any] = {"type": "kafka", "config": self.config.kafka or default_kafka_config()}
        datahub = self.datahub_client_config()
        return {
            "name": self.config.pipelineName,
            **({"datahub": datahub} if datahub else {}),
            "source": source,
            "filter": {"event_type": "EntityChangeEvent_v1", "event": {"entityType": "actionRequest", "category": "LIFECYCLE"}},
            "action": {
                "type": "workflow_actions",
                "config": {
                    "schemaVersion": self.rules.schemaVersion,
                    "rules": [r.model_dump(exclude_none=True) for r in self.rules.rules],
                    **({"statePath": self.config.statePath} if self.config.statePath else {}),
                    **({"connections": self.config.connections} if self.config.connections else {}),
                    **({"runHistory": self.config.runHistory} if self.config.runHistory else {}),
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
            "workflow-actions: starting actions pipeline with %s rule(s) against %s",
            len(self.rules.rules),
            (pipeline_config.get("datahub") or {}).get("server", "no DataHub"),
        )
        pipeline = Pipeline.create(pipeline_config)
        pipeline.run()  # blocks until stopped
        return iter(())

    def get_report(self) -> Any:
        return self.report

    def close(self) -> None:
        pass
