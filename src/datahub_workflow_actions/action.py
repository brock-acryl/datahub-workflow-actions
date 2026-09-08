"""datahub-actions adapter: ``action: { type: workflow_actions, config: {...} }``.

Config is the rules block (``schemaVersion``, ``rules``) plus optional
``statePath`` (sqlite idempotency store), ``dryRun``, ``rulesFile`` (load the
rules from a JSON/YAML file instead of inline)."""

from __future__ import annotations

import json
import logging
from typing import Any, Dict, List, Mapping, Optional, Tuple

from datahub_workflow_actions.context import (
    GraphResolver,
    build_event_context,
    build_workflow_context,
    event_view,
    is_workflow_lifecycle_event,
)
from datahub_workflow_actions.contract import EventTrigger, Rule, RulesConfig, load_rules
from datahub_workflow_actions.dispatch import RecentEvents, RuleRateLimiter, limits_from_config
from datahub_workflow_actions.engine import Engine, RuleRun, event_trigger_matches
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


def event_payload(inner: Any) -> Dict[str, Any]:
    """EntityChangeEvent as a plain dict *including* ``parameters``. datahub-actions
    keeps the free-form parameters outside the Avro record (``__parameters_json``),
    so ``to_obj()`` alone silently drops workflowUrn, result, fields, …"""
    payload: Dict[str, Any] = inner.to_obj() if hasattr(inner, "to_obj") else dict(inner)
    params = getattr(inner, "safe_parameters", None)
    if params is None and isinstance(inner, Mapping):
        params = inner.get("parameters")
    if params:
        payload["parameters"] = dict(params)
    return payload


OWN_ACTOR_QUERY = "query workflowActionsMe { me { corpUser { urn } } }"


class EventIndex:
    """§21 Event rules bucketed by category. ``candidates`` runs the lookup-free trigger checks
    (category → operation → entity type → own actor → modifier → parameters) so a non-matching
    event costs one dict lookup and never a GraphQL call. Rebuilt on every hot reload."""

    def __init__(self, rules: List[Rule]):
        self.buckets: Dict[str, List[Rule]] = {}
        for rule in rules:
            if isinstance(rule.on, EventTrigger):
                self.buckets.setdefault(rule.on.category, []).append(rule)

    def __len__(self) -> int:
        return sum(len(rules) for rules in self.buckets.values())

    def candidates(self, view: Mapping[str, Any], own_actor: Optional[str]) -> Tuple[List[Rule], List[Tuple[str, str]]]:
        """(rules whose trigger matches, [(rule id, reason)] for the ones that don't)."""
        matched: List[Rule] = []
        rejected: List[Tuple[str, str]] = []
        for rule in self.buckets.get(str(view.get("category") or "").upper(), []):
            reason = event_trigger_matches(rule.on, view, own_actor)  # type: ignore[arg-type]
            (matched.append(rule) if reason is None else rejected.append((rule.id, reason)))
        return matched, rejected

    def describe(self) -> str:
        if not self.buckets:
            return "no event rules"
        return ", ".join(f"{category}:{len(rules)}" for category, rules in sorted(self.buckets.items()))


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
        own_actor: Optional[str] = None,
        recent: Optional[RecentEvents] = None,
        limiter: Optional[RuleRateLimiter] = None,
    ):
        self.config = config
        self.ctx = ctx
        self.index = EventIndex(config.rules)
        self.recent = recent if recent is not None else RecentEvents()
        self.limiter = limiter if limiter is not None else RuleRateLimiter(None)
        self._own_actor: Optional[str] = own_actor
        self._own_actor_resolved = own_actor is not None
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
        logger.info("workflow-actions: loaded %s", self._describe(config, dry_run))

    def _describe(self, config: RulesConfig, dry_run: bool) -> str:
        workflow_rules = config.workflow_rules()
        return "%s rule(s) — %s workflow rule(s) for %s workflow(s), %s event rule(s) [%s]; dedupe %s, %s%s" % (
            len(config.rules),
            len(workflow_rules),
            len({r.workflowUrn for r in workflow_rules}),
            len(self.index),
            self.index.describe(),
            f"{self.recent.window:g}s" if self.recent.enabled else "off",
            self.limiter.describe(),
            " (dry run)" if dry_run else "",
        )

    def own_actor(self) -> Optional[str]:
        """The user this engine writes as (its token's ``me``), resolved once and cached; the
        ``ignoreOwnChanges`` guard compares the event's actor against it. None when unknown —
        the guard then lets every event through rather than dropping any."""
        if self._own_actor_resolved:
            return self._own_actor
        self._own_actor_resolved = True
        graph = self.engine.run_context.graph
        if graph is None:
            return None
        try:
            data = graph.execute_graphql(OWN_ACTOR_QUERY) or {}
            self._own_actor = (((data.get("me") or {}).get("corpUser") or {}).get("urn")) or None
        except Exception as e:  # noqa: BLE001 — best effort
            logger.warning("workflow-actions: could not resolve own actor (ignoreOwnChanges guard is off): %s", e)
            self._own_actor = None
        if self._own_actor:
            logger.info("workflow-actions: own changes are made as %s", self._own_actor)
        return self._own_actor

    def apply_config(self, config_dict: Dict[str, Any]) -> None:
        """Hot reload (§18.24): swap rules, connections and run-history settings from a fresh
        `source.config`. The idempotency state store is kept; an event already being handled
        finishes with the rules it started with (handle_event snapshots them)."""
        from datahub_workflow_actions.connections import ConnectionResolver, validate_connections
        from datahub_workflow_actions.runs import RunRecorder

        new_config = load_config(config_dict)  # raises on an invalid recipe → caller keeps the old one
        connections = config_dict.get("connections")
        for problem in validate_connections(connections):
            logger.warning("workflow-actions: %s", problem)
        graph = self.engine.run_context.graph
        run_context = RunContext(
            graph=graph,
            connections=normalize_connections(connections),
            connection_resolver=ConnectionResolver.from_config(connections, graph=graph),
        )
        dry_run = bool(config_dict.get("dryRun", False))
        self.engine = Engine(run_context, state=self.engine.state, dry_run=dry_run)
        self.recorder = RunRecorder.from_config(graph, config_dict.get("runHistory"))
        self.index = EventIndex(new_config.rules)
        self.recent, self.limiter = limits_from_config(config_dict)
        self.config = new_config
        logger.info("workflow-actions: applied %s", self._describe(new_config, dry_run))

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
            own_actor=config_dict.get("ownActor"),
            recent=limits_from_config(config_dict)[0],
            limiter=limits_from_config(config_dict)[1],
        )

    def act(self, event: Any) -> None:
        if getattr(event, "event_type", None) != ENTITY_CHANGE_EVENT_V1_TYPE:
            return
        self.handle_event(event_payload(event.event))

    def handle_event(self, payload: Dict[str, Any]) -> list:
        """Workflow lifecycle events run the rules of their workflow; every other
        EntityChangeEvent goes through the EventIndex (§21)."""
        config, engine, recorder, index = self.config, self.engine, self.recorder, self.index  # snapshot: a reload mid-event does not mix rule sets
        if is_workflow_lifecycle_event(payload):
            workflow_urn = (payload.get("parameters") or {}).get("workflowUrn")
            candidates = config.rules_for(workflow_urn) if workflow_urn else config.workflow_rules()
            if not candidates:
                return []
            context = build_workflow_context(payload, self._resolver())
        else:
            if not len(index):
                return []
            view = event_view(payload)
            candidates, rejected = index.candidates(view, self.own_actor())
            for rule_id, reason in rejected:
                logger.debug("workflow-actions: rule %s not fired (%s)", rule_id, reason)
            if not candidates:
                return []
            if self.recent.duplicate(view):
                logger.info(
                    "workflow-actions: duplicate %s %s on %s within %gs — skipped for %s",
                    view.get("category"), view.get("operation"), view.get("entityUrn"), self.recent.window,
                    ", ".join(r.id for r in candidates),
                )
                return [RuleRun(ruleId=r.id, fired=False, reason=f"duplicate event within {self.recent.window:g}s") for r in candidates]
            limited = [r for r in candidates if not self.limiter.allow(r.id)]
            candidates = [r for r in candidates if r not in limited]
            for r in limited:
                logger.warning("workflow-actions: rule %s over its limit (%s) — skipped", r.id, self.limiter.describe())
            if not candidates:
                return [RuleRun(ruleId=r.id, fired=False, reason=f"rate limited ({self.limiter.describe()})") for r in limited]
            context = build_event_context(payload, self._resolver())
            context["engine"] = {"actor": self.own_actor()}
            return self._run(config, engine, recorder, candidates, context) + [
                RuleRun(ruleId=r.id, fired=False, reason=f"rate limited ({self.limiter.describe()})") for r in limited
            ]
        return self._run(config, engine, recorder, candidates, context)

    def _resolver(self) -> Any:
        if self.resolver is not None:
            return self.resolver
        from datahub_workflow_actions.context import StaticResolver

        return StaticResolver()

    def _run(self, config: RulesConfig, engine: Engine, recorder: Any, candidates: List[Rule], context: Dict[str, Any]) -> list:
        import time as _time

        started_ms = int(_time.time() * 1000)
        runs = engine.run(RulesConfig(schemaVersion=config.schemaVersion, rules=candidates), context)
        rules_by_id = {r.id: r for r in candidates}
        for run in runs:
            if recorder is not None and run.fired:
                recorder.record(run, rules_by_id.get(run.ruleId), context, started_ms=started_ms)
            if not run.fired:
                logger.info("workflow-actions: rule %s not fired (%s)", run.ruleId, run.reason)
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
