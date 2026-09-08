"""Evaluate rules against a context document and run their steps.

Semantics: steps run in order; ``when`` gates each step; ``forEach`` fans a
step out over a list (each element as ``item``, index as ``index``, with
``itemWhen`` filtering elements and ``batch`` merging them into bulk calls
when the step accepts a list); every
step's output is exposed to later templates as ``steps.<id>``; failures honour
``onError`` (fail the rule / continue / stop), with per-step retry + backoff
and a timeout; idempotency keys skip work already recorded in the state
store. ``dry_run`` renders and plans everything without side effects."""

from __future__ import annotations

import concurrent.futures
import json
import logging
import time
from dataclasses import asdict, dataclass, field
from typing import Any, Callable, Dict, List, Mapping, Optional

from datahub_workflow_actions.contract import EventTrigger, Filter, Rule, RulesConfig, Step, ValueMatch
from datahub_workflow_actions.filters import _matches_one, evaluate, evaluate_filter
from datahub_workflow_actions.state import InMemoryStateStore
from datahub_workflow_actions.steps import RunContext, get_step
from datahub_workflow_actions.templating import TemplateError, render_params, render_value

logger = logging.getLogger("datahub_workflow_actions.engine")


@dataclass
class StepRun:
    stepId: str
    type: str
    status: str  # ok | skipped | failed | dry-run | timed-out
    reason: Optional[str] = None
    attempts: int = 0
    params: Dict[str, Any] = field(default_factory=dict)
    output: Any = None
    error: Optional[str] = None
    items: Optional[List["StepRun"]] = None
    idempotencyKey: Optional[str] = None

    def ok(self) -> bool:
        return self.status in ("ok", "dry-run", "skipped")


@dataclass
class RuleRun:
    ruleId: str
    fired: bool
    reason: Optional[str] = None
    status: str = "not-fired"  # ok | failed | stopped | not-fired | dry-run
    steps: List[StepRun] = field(default_factory=list)

    def to_dict(self) -> dict:
        return asdict(self)


def _value_matches(actual: Any, match: ValueMatch) -> bool:
    probe = Filter(field="modifier", condition=match.condition, values=match.values, negated=False, caseInsensitive=match.caseInsensitive)
    if match.condition == "EXISTS":
        result = actual not in (None, "", [], {})
    elif actual is None:
        result = False
    else:
        result = _matches_one(actual, probe)
    return (not result) if match.negated else result


def event_trigger_matches(trigger: EventTrigger, event: Mapping[str, Any], own_actor: Optional[str] = None) -> Optional[str]:
    """§21 — None when an EntityChangeEvent view ({entityType, category, operation, modifier,
    parameters, actor}) satisfies the trigger; otherwise the reason. Cheap checks first, so this
    can run on every event before any lookup."""
    category = str(event.get("category") or "").upper()
    if category != trigger.category:
        return f"category {category or '?'} != {trigger.category}"
    operation = str(event.get("operation") or "").upper()
    if trigger.operations and operation not in trigger.operations:
        return f"operation {operation or '?'} not in {trigger.operations}"
    entity_type = str(event.get("entityType") or "")
    if trigger.entityTypes and entity_type.lower() not in {t.lower() for t in trigger.entityTypes}:
        return f"entity type {entity_type or '?'} not in {trigger.entityTypes}"
    actor = event.get("actor")
    if trigger.ignoreOwnChanges and own_actor and actor == own_actor:
        return f"own change (actor {actor})"
    if trigger.modifier is not None and not _value_matches(event.get("modifier"), trigger.modifier):
        return f"modifier {event.get('modifier')} does not match"
    parameters = event.get("parameters") or {}
    for condition in trigger.parameters:
        if not evaluate_filter(condition, parameters):
            return f"parameter {condition.field} does not match"
    return None


def trigger_matches(rule: Rule, context: Mapping[str, Any]) -> Optional[str]:
    """None when the rule's trigger matches the event; otherwise the reason it doesn't."""
    event = context.get("event") or {}
    if isinstance(rule.on, EventTrigger):
        if event.get("type") == "Schedule":
            return "a schedule tick, not a change event"
        if event.get("category") is None:
            return "not a change event"
        own_actor = (context.get("engine") or {}).get("actor")
        return event_trigger_matches(rule.on, event, own_actor)
    workflow_urn = (context.get("workflow") or {}).get("urn")
    if workflow_urn and rule.workflowUrn != workflow_urn:
        return f"workflow {workflow_urn} != {rule.workflowUrn}"
    if not workflow_urn:
        return "event has no workflow urn"
    if rule.on.operation != event.get("operation"):
        return f"operation {event.get('operation')} != {rule.on.operation}"
    if rule.on.result and rule.on.result != event.get("result"):
        return f"result {event.get('result')} != {rule.on.result}"
    if rule.on.stepId and rule.on.stepId != event.get("stepId"):
        return f"step {event.get('stepId')} != {rule.on.stepId}"
    return None


class Engine:
    def __init__(
        self,
        run_context: Optional[RunContext] = None,
        state: Any = None,
        dry_run: bool = False,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.run_context = run_context or RunContext()
        self.run_context.dry_run = self.run_context.dry_run or dry_run
        self.state = state if state is not None else InMemoryStateStore()
        self.dry_run = self.run_context.dry_run
        self.sleep = sleep

    # ---- evaluation -------------------------------------------------------

    def matching_rules(self, config: RulesConfig, context: Mapping[str, Any]) -> List[Rule]:
        return [rule for rule in config.rules if rule.enabled and trigger_matches(rule, context) is None and evaluate(rule.when, context)]

    def run(self, config: RulesConfig, context: Mapping[str, Any]) -> List[RuleRun]:
        runs: List[RuleRun] = []
        for rule in config.rules:
            if not rule.enabled:
                runs.append(RuleRun(rule.id, False, "disabled"))
                continue
            mismatch = trigger_matches(rule, context)
            if mismatch:
                runs.append(RuleRun(rule.id, False, mismatch))
                continue
            if not evaluate(rule.when, context):
                runs.append(RuleRun(rule.id, False, "conditions not met"))
                continue
            runs.append(self.run_rule(rule, context))
        return runs

    def run_rule(self, rule: Rule, context: Mapping[str, Any]) -> RuleRun:
        run = RuleRun(rule.id, True, status="dry-run" if self.dry_run else "ok")
        outputs: Dict[str, Any] = {}
        rule_context: Dict[str, Any] = {**context, "steps": outputs, "rule": {"id": rule.id, "name": rule.name}}
        for step in rule.steps:
            step_run = self.run_step(rule, step, rule_context)
            run.steps.append(step_run)
            outputs[step.id] = {
                "status": step_run.status,
                "output": step_run.output,
                "items": [asdict(i) for i in step_run.items] if step_run.items else None,
            }
            if step_run.status in ("failed", "timed-out"):
                policy = step.onError or rule.onError
                if policy == "continue":
                    continue
                run.status = "stopped" if policy == "stop" else "failed"
                run.reason = f"step '{step.id}' {step_run.status}: {step_run.error}"
                break
        return run

    # ---- steps ------------------------------------------------------------

    def run_step(self, rule: Rule, step: Step, context: Mapping[str, Any]) -> StepRun:
        if not step.enabled:
            return StepRun(step.id, step.type, "skipped", reason="disabled")
        if step.when is not None and not evaluate(step.when, context):
            return StepRun(step.id, step.type, "skipped", reason="conditions not met")
        try:
            definition = get_step(step.type)
        except KeyError as e:
            return StepRun(step.id, step.type, "failed", error=str(e))

        if definition.validate_template:
            problems = definition.validate_template(step.params)
            if problems:
                return StepRun(step.id, step.type, "failed", error="; ".join(problems))

        if step.forEach:
            try:
                items = render_value(step.forEach, context) if "{{" in step.forEach else _path_items(context, step.forEach)
            except TemplateError as e:
                return StepRun(step.id, step.type, "failed", error=f"forEach: {e}")
            if items is None:
                items = []
            if not isinstance(items, list):
                items = [items]
            indexed = list(enumerate(items))

            # Per-item conditions (§19): `item` / `index` are in scope; misses are recorded, not run.
            skipped: List[StepRun] = []
            if step.itemWhen is not None:
                kept = []
                for index, item in indexed:
                    if evaluate(step.itemWhen, {**context, "item": item, "index": index}):
                        kept.append((index, item))
                    else:
                        skipped.append(StepRun(step.id, step.type, "skipped", reason=f"item {index}: conditions not met"))
                indexed = kept

            # Smart batching (§19): a step that accepts a list gets one call per chunk of items
            # whose other params agree, instead of one call per item.
            mode = step.batch.mode if step.batch else "auto"
            size = step.batch.size if step.batch else 100
            batched = mode == "auto" and bool(definition.bulk_param) and len(indexed) > 1
            if batched:
                item_runs = self._run_batched(rule, step, definition, context, indexed, size)
            else:
                item_runs = [
                    self._run_once(rule, step, definition, {**context, "item": item, "index": index}, index)
                    for index, item in indexed
                ]
            failed = [r for r in item_runs if not r.ok()]
            status = "failed" if failed else ("dry-run" if self.dry_run else "ok")
            reason = f"{len(items)} item(s)"
            if skipped:
                reason += f", {len(skipped)} skipped"
            if batched:
                reason += f", {len(item_runs)} call(s)"
            return StepRun(
                step.id,
                step.type,
                status,
                reason=reason,
                attempts=sum(r.attempts for r in item_runs),
                output=[r.output for r in item_runs],
                error="; ".join(f"[{r.reason}] {r.error}" for r in failed) or None,
                items=item_runs + skipped,
            )
        return self._run_once(rule, step, definition, context, None)

    def _run_batched(self, rule: Rule, step: Step, definition, context: Mapping[str, Any], indexed, size: int) -> List[StepRun]:
        bulk = definition.bulk_param
        groups: Dict[str, List[tuple]] = {}
        order: List[str] = []
        runs: List[StepRun] = []
        for index, item in indexed:
            try:
                rendered = render_params(step.params, {**context, "item": item, "index": index})
            except TemplateError as e:
                runs.append(StepRun(step.id, step.type, "failed", reason=f"item {index}", error=f"invalid params: {e}"))
                continue
            key = json.dumps({k: v for k, v in rendered.items() if k != bulk}, sort_keys=True, default=str)
            if key not in groups:
                order.append(key)
            groups.setdefault(key, []).append((index, item, rendered))
        batch_no = 0
        for key in order:
            members = groups[key]
            for start in range(0, len(members), size):
                chunk = members[start : start + size]
                batch_no += 1
                values: List[Any] = []
                for _, _, rendered in chunk:
                    value = rendered.get(bulk)
                    if isinstance(value, list):
                        values.extend(value)
                    elif value is not None and value != "":
                        values.append(value)
                first_index, first_item, first_rendered = chunk[0]
                batch_context = {
                    **context,
                    "items": [member[1] for member in chunk],
                    "item": first_item,
                    "index": first_index,
                    "batch": batch_no,
                }
                runs.append(
                    self._run_once(
                        rule,
                        step,
                        definition,
                        batch_context,
                        first_index,
                        rendered={**first_rendered, bulk: values},
                        label=f"batch {batch_no}: {len(chunk)} item(s)",
                        key_suffix=f"batch{batch_no}",
                    )
                )
        return runs

    def _run_once(
        self,
        rule: Rule,
        step: Step,
        definition,
        context: Mapping[str, Any],
        index: Optional[int],
        *,
        rendered: Optional[Dict[str, Any]] = None,
        label: Optional[str] = None,
        key_suffix: Optional[str] = None,
    ) -> StepRun:
        event_id = (context.get("event") or {}).get("id", "no-event")
        label = label or (f"item {index}" if index is not None else None)
        try:
            if step.idempotencyKey:
                key = str(render_value(step.idempotencyKey, context))
            else:
                suffix = key_suffix or (str(index) if index is not None else None)
                key = f"{event_id}|{rule.id}|{step.id}" + (f"|{suffix}" if suffix is not None else "")
            if rendered is None:
                rendered = render_params(step.params, context)
            params = definition.params.model_validate(rendered)
        except (TemplateError, ValueError) as e:
            return StepRun(step.id, step.type, "failed", reason=label, error=f"invalid params: {e}")

        if self.state.seen(key) and not self.dry_run:
            return StepRun(step.id, step.type, "skipped", reason="already ran (idempotency key)", params=rendered, idempotencyKey=key, output=self.state.get(key))

        attempts_allowed = step.retry.attempts if step.retry else 1
        run_ctx = RunContext(
            graph=self.run_context.graph,
            http=self.run_context.http,
            dry_run=self.dry_run,
            timeout=step.timeoutSeconds or self.run_context.timeout,
            sleep=self.run_context.sleep,
            env=self.run_context.env,
            context=dict(context),
            connections=self.run_context.connections,
            connection_resolver=self.run_context.connection_resolver,
        )
        last_error: Optional[str] = None
        status = "failed"
        for attempt in range(1, attempts_allowed + 1):
            try:
                output = self._call_with_timeout(definition.run, params, run_ctx, step.timeoutSeconds)
                if not self.dry_run:
                    self.state.mark(key, output)
                return StepRun(
                    step.id, step.type, "dry-run" if self.dry_run else "ok",
                    reason=label,
                    attempts=attempt, params=rendered, output=output, idempotencyKey=key,
                )
            except concurrent.futures.TimeoutError:
                last_error = f"timed out after {step.timeoutSeconds}s"
                status = "timed-out"
            except Exception as e:  # noqa: BLE001 — step failures are reported, not raised
                last_error = f"{type(e).__name__}: {e}"
                status = "failed"
            if attempt < attempts_allowed and step.retry:
                delay = step.retry.delaySeconds * (2 ** (attempt - 1) if step.retry.backoff == "exponential" else 1)
                self.sleep(min(delay, step.retry.maxDelaySeconds))
                logger.info("workflow-actions: retrying step %s (attempt %s/%s)", step.id, attempt + 1, attempts_allowed)
        return StepRun(
            step.id, step.type, status,
            reason=label,
            attempts=attempts_allowed, params=rendered, error=last_error, idempotencyKey=key,
        )

    @staticmethod
    def _call_with_timeout(fn: Callable, params: Any, ctx: RunContext, timeout: Optional[float]) -> Any:
        if not timeout:
            return fn(params, ctx)
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(fn, params, ctx)
            return future.result(timeout=timeout)


def _path_items(context: Mapping[str, Any], path: str) -> Any:
    from datahub_workflow_actions.filters import resolve_path

    found, value = resolve_path(context, path)
    return value if found else []
