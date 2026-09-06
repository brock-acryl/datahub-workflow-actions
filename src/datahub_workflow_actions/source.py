"""Ingestion-recipe wrapper so the same engine runs from the recipe the MFE
writes (``source.type: datahub-workflow-actions``) on a local or remote
executor. It starts a datahub-actions pipeline in-process: a Kafka
EntityChangeEvent source (connection under ``source.config.kafka``, defaulting
to the executor's usual env) feeding ``WorkflowActionsAction`` with the rules
block. Runs until the process is stopped; emits no metadata itself."""

from __future__ import annotations

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


class WorkflowActionsSourceConfig(ConfigModel):  # type: ignore[misc]
    model_config = ConfigDict(extra="allow")

    schemaVersion: int = 1
    rules: list = Field(default_factory=list)
    kafka: Optional[Dict[str, Any]] = Field(None, description="datahub-actions kafka source config (connection, topic_routes).")
    statePath: Optional[str] = None
    dryRun: bool = False
    pipelineName: str = "workflow-actions"


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

    def actions_pipeline_config(self) -> Dict[str, Any]:
        source: Dict[str, Any] = {"type": "kafka"}
        if self.config.kafka:
            source["config"] = self.config.kafka
        return {
            "name": self.config.pipelineName,
            "source": source,
            "filter": {"event_type": "EntityChangeEvent_v1", "event": {"entityType": "actionRequest", "category": "LIFECYCLE"}},
            "action": {
                "type": "workflow_actions",
                "config": {
                    "schemaVersion": self.rules.schemaVersion,
                    "rules": [r.model_dump(exclude_none=True) for r in self.rules.rules],
                    **({"statePath": self.config.statePath} if self.config.statePath else {}),
                    "dryRun": self.config.dryRun,
                },
            },
        }

    def get_workunits(self) -> Iterable[Any]:
        from datahub_actions.pipeline.pipeline import Pipeline

        pipeline_config = self.actions_pipeline_config()
        logger.info("workflow-actions: starting actions pipeline with %s rule(s)", len(self.rules.rules))
        pipeline = Pipeline.create(pipeline_config)
        pipeline.run()  # blocks until stopped
        return iter(())

    def get_report(self) -> Any:
        return self.report

    def close(self) -> None:
        pass
