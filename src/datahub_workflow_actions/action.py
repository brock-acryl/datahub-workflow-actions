"""datahub-actions adapter: ``action: { type: workflow_actions, config: {...} }``.

Config is the rules block (``schemaVersion``, ``rules``) plus optional
``statePath`` (sqlite idempotency store), ``dryRun``, ``rulesFile`` (load the
rules from a JSON/YAML file instead of inline)."""

from __future__ import annotations

import json
import logging
from typing import Any, Dict, Optional

from datahub_workflow_actions.context import GraphResolver, build_context, is_workflow_lifecycle_event
from datahub_workflow_actions.contract import RulesConfig, load_rules
from datahub_workflow_actions.engine import Engine
from datahub_workflow_actions.state import InMemoryStateStore, SqliteStateStore, default_state_path
from datahub_workflow_actions.steps import RunContext

logger = logging.getLogger("datahub_workflow_actions.action")

try:  # datahub-actions is a runtime dependency, but keep the engine importable without it
    from datahub_actions.action.action import Action
    from datahub_actions.event.event_envelope import EventEnvelope
    from datahub_actions.event.event_registry import ENTITY_CHANGE_EVENT_V1_TYPE
    from datahub_actions.pipeline.pipeline_context import PipelineContext
except ImportError:  # pragma: no cover
    Action = object  # type: ignore[misc,assignment]
    EventEnvelope = Any  # type: ignore[misc,assignment]
    PipelineContext = Any  # type: ignore[misc,assignment]
    ENTITY_CHANGE_EVENT_V1_TYPE = "EntityChangeEvent_v1"


def normalize_connections(raw: Optional[Dict[str, Any]]) -> Dict[str, str]:
    """Plain ``url`` connections as ``{name: url}`` with ``${VAR}`` resolved from the environment.
    Kept for callers that only need URLs; the engine uses ``ConnectionResolver`` (all kinds)."""
    from datahub_workflow_actions.connections import parse_connections, substitute_env

    return {name: substitute_env(spec.url or "") for name, spec in parse_connections(raw).items() if spec.kind == "url" and spec.url}


def load_config(config_dict: Dict[str, Any]) -> RulesConfig:
    rules_file = config_dict.get("rulesFile")
    if rules_file:
        with open(rules_file, "r", encoding="utf-8") as handle:
            text = handle.read()
        try:
            data = json.loads(text)
        except json.JSONDecodeError:
            import yaml

            data = yaml.safe_load(text)
        return load_rules(data)
    return load_rules(config_dict)


class WorkflowActionsAction(Action):  # type: ignore[misc]
    def __init__(
        self,
        config: RulesConfig,
        ctx: Any,
        *,
        state_path: Optional[str] = None,
        dry_run: bool = False,
        in_memory_state: bool = False,
        connections: Optional[Dict[str, Any]] = None,
        run_history: Optional[Dict[str, Any]] = None,
    ):
        self.config = config
        self.ctx = ctx
        graph = getattr(getattr(ctx, "graph", None), "graph", None)
        self.resolver = GraphResolver(graph) if graph is not None else None
        state = InMemoryStateStore() if in_memory_state else SqliteStateStore(state_path or default_state_path())
        from datahub_workflow_actions.connections import ConnectionResolver, validate_connections

        for problem in validate_connections(connections):
            logger.warning("workflow-actions: %s", problem)
        run_context = RunContext(
            graph=graph,
            connections=normalize_connections(connections),
            connection_resolver=ConnectionResolver.from_config(connections, graph=graph),
        )
        self.engine = Engine(run_context, state=state, dry_run=dry_run)
        from datahub_workflow_actions.runs import RunRecorder

        self.recorder = RunRecorder.from_config(graph, run_history)
        logger.info(
            "workflow-actions: loaded %s rule(s) for %s workflow(s)%s",
            len(config.rules),
            len({r.workflowUrn for r in config.rules}),
            " (dry run)" if dry_run else "",
        )

    @classmethod
    def create(cls, config_dict: dict, ctx: Any) -> "WorkflowActionsAction":
        config_dict = config_dict or {}
        return cls(
            load_config(config_dict),
            ctx,
            state_path=config_dict.get("statePath"),
            dry_run=bool(config_dict.get("dryRun", False)),
            in_memory_state=bool(config_dict.get("inMemoryState", False)),
            connections=config_dict.get("connections"),
            run_history=config_dict.get("runHistory"),
        )

    def act(self, event: Any) -> None:
        if getattr(event, "event_type", None) != ENTITY_CHANGE_EVENT_V1_TYPE:
            return
        payload = event.event.to_obj() if hasattr(event.event, "to_obj") else dict(event.event)
        self.handle_event(payload)

    def handle_event(self, payload: Dict[str, Any]) -> list:
        if not is_workflow_lifecycle_event(payload):
            return []
        workflow_urn = (payload.get("parameters") or {}).get("workflowUrn")
        candidates = self.config.rules_for(workflow_urn) if workflow_urn else self.config.rules
        if not candidates:
            return []
        resolver = self.resolver
        if resolver is None:
            from datahub_workflow_actions.context import StaticResolver

            resolver = StaticResolver()
        context = build_context(payload, resolver)
        import time as _time

        started_ms = int(_time.time() * 1000)
        runs = self.engine.run(RulesConfig(schemaVersion=self.config.schemaVersion, rules=candidates), context)
        rules_by_id = {r.id: r for r in candidates}
        for run in runs:
            if self.recorder is not None and run.fired:
                self.recorder.record(run, rules_by_id.get(run.ruleId), context, started_ms=started_ms)
            if run.fired:
                logger.info("workflow-actions: rule %s → %s (%s)", run.ruleId, run.status, run.reason or f"{len(run.steps)} step(s)")
                for step in run.steps:
                    level = logging.INFO if step.ok() else logging.ERROR
                    logger.log(level, "  step %s [%s] %s %s", step.stepId, step.type, step.status, step.error or "")
        return runs

    def close(self) -> None:
        state = getattr(self.engine, "state", None)
        if hasattr(state, "close"):
            state.close()
