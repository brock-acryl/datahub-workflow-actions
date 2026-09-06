"""Evaluate rules against a context document and run their steps.

Semantics: steps run in order; ``when`` gates each step; ``forEach`` fans a
step out over a list (each element as ``item``, index as ``index``); every
step's output is exposed to later templates as ``steps.<id>``; failures honour
``onError`` (fail the rule / continue / stop), with per-step retry + backoff
and a timeout; idempotency keys skip work already recorded in the state
store. ``dry_run`` renders and plans everything without side effects."""

from __future__ import annotations

import concurrent.futures
import logging
import time
from dataclasses import asdict, dataclass, field
from typing import Any, Callable, Dict, List, Mapping, Optional

from datahub_workflow_actions.contract import Rule, RulesConfig, Step
from datahub_workflow_actions.filters import evaluate
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


def trigger_matches(rule: Rule, context: Mapping[str, Any]) -> Optional[str]:
    """None when the rule's trigger matches the event; otherwise the reason it doesn't."""
    event = context.get("event") or {}
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

        if step.forEach:
            try:
                items = render_value(step.forEach, context) if "{{" in step.forEach else _path_items(context, step.forEach)
            except TemplateError as e:
                return StepRun(step.id, step.type, "failed", error=f"forEach: {e}")
            if items is None:
                items = []
            if not isinstance(items, list):
                items = [items]
            item_runs = [
                self._run_once(rule, step, definition, {**context, "item": item, "index": index}, index)
                for index, item in enumerate(items)
            ]
            failed = [r for r in item_runs if not r.ok()]
            status = "failed" if failed else ("dry-run" if self.dry_run else "ok")
            return StepRun(
                step.id,
                step.type,
                status,
                reason=f"{len(items)} item(s)",
                attempts=sum(r.attempts for r in item_runs),
                output=[r.output for r in item_runs],
                error="; ".join(f"[{r.reason}] {r.error}" for r in failed) or None,
                items=item_runs,
            )
        return self._run_once(rule, step, definition, context, None)

    def _run_once(self, rule: Rule, step: Step, definition, context: Mapping[str, Any], index: Optional[int]) -> StepRun:
        event_id = (context.get("event") or {}).get("id", "no-event")
        try:
            if step.idempotencyKey:
                key = str(render_value(step.idempotencyKey, context))
            else:
                key = f"{event_id}|{rule.id}|{step.id}" + (f"|{index}" if index is not None else "")
            rendered = render_params(step.params, context)
            params = definition.params.model_validate(rendered)
        except (TemplateError, ValueError) as e:
            return StepRun(step.id, step.type, "failed", reason=f"item {index}" if index is not None else None, error=f"invalid params: {e}")

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
                    reason=f"item {index}" if index is not None else None,
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
            reason=f"item {index}" if index is not None else None,
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
