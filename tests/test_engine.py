import concurrent.futures

import pytest

from datahub_workflow_actions.contract import RulesConfig
from datahub_workflow_actions.engine import Engine, trigger_matches
from datahub_workflow_actions.state import InMemoryStateStore
from datahub_workflow_actions.steps import RunContext, StepParams, step
from tests.conftest import DATASET, WF

CALLS = []


class RecordParams(StepParams):
    value: str = "x"
    fail_times: int = 0


@step("_record", label="Record", description="test", group="Test", params=RecordParams, outputs={"echo": "value"})
def _record(p: RecordParams, ctx: RunContext):
    CALLS.append(p.value)
    remaining = sum(1 for c in CALLS if c == p.value)
    if remaining <= p.fail_times:
        raise RuntimeError(f"boom {remaining}")
    return {"echo": p.value}


class SlowParams(StepParams):
    seconds: float = 0.5


@step("_slow", label="Slow", description="test", group="Test", params=SlowParams)
def _slow(p: SlowParams, ctx: RunContext):
    import time

    time.sleep(p.seconds)
    return {"done": True}


def rule(**overrides):
    base = {"id": "r1", "workflowUrn": WF, "on": {"operation": "COMPLETED", "result": "ACCEPTED"}, "steps": []}
    base.update(overrides)
    return base


def cfg(*rules):
    return RulesConfig(rules=list(rules))


@pytest.fixture(autouse=True)
def _reset_calls():
    CALLS.clear()


def test_trigger_matching(context):
    assert trigger_matches(RulesConfig(rules=[rule()]).rules[0], context) is None
    assert "result" in trigger_matches(RulesConfig(rules=[rule(on={"operation": "COMPLETED", "result": "REJECTED"})]).rules[0], context)
    assert "operation" in trigger_matches(RulesConfig(rules=[rule(on={"operation": "CREATE"})]).rules[0], context)
    assert "workflow" in trigger_matches(RulesConfig(rules=[rule(workflowUrn="urn:li:actionWorkflow:other")]).rules[0], context)


def test_run_orders_steps_exposes_outputs_and_reports(context):
    config = cfg(
        rule(
            steps=[
                {"id": "a", "type": "_record", "params": {"value": "first"}},
                {"id": "b", "type": "_record", "params": {"value": "after {{ steps.a.output.echo }}"}},
            ]
        )
    )
    runs = Engine(state=InMemoryStateStore()).run(config, context)
    assert runs[0].fired and runs[0].status == "ok"
    assert [s.status for s in runs[0].steps] == ["ok", "ok"]
    assert CALLS == ["first", "after first"]
    assert runs[0].steps[1].output == {"echo": "after first"}


def test_rule_level_conditions_and_disabled_rules(context):
    config = cfg(
        rule(id="off", enabled=False, steps=[{"id": "a", "type": "_record"}]),
        rule(id="nomatch", when={"operator": "AND", "filters": [{"field": "form.field_abc", "values": ["public"]}]}, steps=[{"id": "a", "type": "_record"}]),
        rule(id="match", when={"operator": "AND", "filters": [{"field": "form.field_abc", "values": ["restricted"]}]}, steps=[{"id": "a", "type": "_record"}]),
    )
    runs = Engine().run(config, context)
    assert [(r.ruleId, r.fired, r.reason) for r in runs] == [("off", False, "disabled"), ("nomatch", False, "conditions not met"), ("match", True, None)]
    assert CALLS == ["x"]


def test_step_conditions_disabled_steps_and_unknown_types(context):
    config = cfg(
        rule(
            onError="continue",
            steps=[
                {"id": "skip", "type": "_record", "when": {"operator": "AND", "filters": [{"field": "entity.type", "values": ["chart"]}]}},
                {"id": "off", "type": "_record", "enabled": False},
                {"id": "unknown", "type": "nope"},
                {"id": "run", "type": "_record", "params": {"value": "ran"}},
            ],
        )
    )
    run = Engine().run(config, context)[0]
    assert [s.status for s in run.steps] == ["skipped", "skipped", "failed", "ok"]
    assert run.status == "ok" and CALLS == ["ran"]


def test_on_error_fail_stop_continue(context):
    failing = {"id": "bad", "type": "_record", "params": {"value": "bad", "fail_times": 5}}
    after = {"id": "after", "type": "_record", "params": {"value": "after"}}
    fail_run = Engine().run(cfg(rule(steps=[failing, after])), context)[0]
    assert fail_run.status == "failed" and [s.stepId for s in fail_run.steps] == ["bad"] and "boom" in fail_run.reason
    CALLS.clear()
    stop_run = Engine().run(cfg(rule(steps=[{**failing, "onError": "stop"}, after])), context)[0]
    assert stop_run.status == "stopped" and len(stop_run.steps) == 1
    CALLS.clear()
    cont_run = Engine().run(cfg(rule(steps=[{**failing, "onError": "continue"}, after])), context)[0]
    assert cont_run.status == "ok" and [s.status for s in cont_run.steps] == ["failed", "ok"]


def test_retries_with_backoff(context):
    sleeps = []
    config = cfg(rule(steps=[{"id": "flaky", "type": "_record", "params": {"value": "flaky", "fail_times": 2}, "retry": {"attempts": 3, "backoff": "exponential", "delaySeconds": 1, "maxDelaySeconds": 10}}]))
    run = Engine(sleep=sleeps.append).run(config, context)[0]
    assert run.status == "ok" and run.steps[0].attempts == 3 and sleeps == [1, 2]
    CALLS.clear()
    config = cfg(rule(steps=[{"id": "flaky", "type": "_record", "params": {"value": "flaky2", "fail_times": 5}, "retry": {"attempts": 2, "backoff": "fixed", "delaySeconds": 0.5}}]))
    run = Engine(sleep=sleeps.append).run(config, context)[0]
    assert run.status == "failed" and run.steps[0].attempts == 2 and sleeps[-1] == 0.5


def test_timeout(context):
    config = cfg(rule(steps=[{"id": "slow", "type": "_slow", "params": {"seconds": 0.5}, "timeoutSeconds": 0.05}]))
    run = Engine().run(config, context)[0]
    assert run.steps[0].status == "timed-out" and "timed out" in run.reason


def test_for_each_fans_out_with_item_and_index(context):
    config = cfg(rule(steps=[{"id": "each", "type": "_record", "forEach": "{{ entity.owners }}", "params": {"value": "{{ index }}:{{ item | urn_name }}"}}]))
    run = Engine().run(config, context)[0]
    assert run.status == "ok" and run.steps[0].reason == "2 item(s)"
    assert CALLS == ["0:owner1", "1:data-eng"]
    assert [i.output for i in run.steps[0].items] == [{"echo": "0:owner1"}, {"echo": "1:data-eng"}]
    # path form and empty list
    config = cfg(rule(steps=[{"id": "each", "type": "_record", "forEach": "entity.tags", "params": {"value": "{{ item }}"}}]))
    run = Engine().run(config, context)[0]
    assert run.steps[0].reason == "1 item(s)"  # tags has one element in the fixture


def test_idempotency_skips_repeats_and_honours_custom_key(context):
    state = InMemoryStateStore()
    config = cfg(rule(steps=[{"id": "once", "type": "_record", "params": {"value": "once"}}]))
    first = Engine(state=state).run(config, context)[0]
    second = Engine(state=state).run(config, context)[0]
    assert first.steps[0].status == "ok" and second.steps[0].status == "skipped"
    assert second.steps[0].reason.startswith("already ran") and second.steps[0].output == {"echo": "once"}
    assert CALLS == ["once"]
    custom = cfg(rule(steps=[{"id": "k", "type": "_record", "idempotencyKey": "{{ entity.urn }}|grant", "params": {"value": "k"}}]))
    run = Engine(state=state).run(custom, context)[0]
    assert run.steps[0].idempotencyKey == f"{DATASET}|grant"


def test_dry_run_renders_but_does_not_execute_or_record(context):
    state = InMemoryStateStore()
    config = cfg(rule(steps=[{"id": "tag", "type": "add_tag", "params": {"entity": "{{ entity.urn }}", "tag": "urn:li:tag:x"}}]))
    run = Engine(state=state, dry_run=True).run(config, context)[0]
    assert run.status == "dry-run" and run.steps[0].status == "dry-run"
    assert run.steps[0].output["mutation"] == "batchAddTags" and run.steps[0].output["variables"]["input"]["resources"] == [{"resourceUrn": DATASET}]
    assert not state.seen(run.steps[0].idempotencyKey)


def test_invalid_rendered_params_fail_the_step(context):
    config = cfg(rule(steps=[{"id": "bad", "type": "add_tag", "params": {"entity": "{{ entity.urn }}", "tag": "{{ form.nope }}"}}]))
    run = Engine().run(config, context)[0]
    assert run.steps[0].status == "failed" and "invalid params" in run.steps[0].error


# ---- §19: per-item conditions and smart batching -----------------------------


def _for_each_rule(step_extra=None, params=None):
    from datahub_workflow_actions.contract import Rule

    return Rule.model_validate(
        {
            "id": "r",
            "workflowUrn": "urn:li:actionWorkflow:w",
            "on": {"operation": "COMPLETED", "result": "ACCEPTED"},
            "steps": [
                {
                    "id": "tag",
                    "type": "add_tag",
                    "forEach": "{{ lookup.urns }}",
                    "params": params or {"entity": "{{ item }}", "tag": "urn:li:tag:pii"},
                    **(step_extra or {}),
                }
            ],
        }
    )


def _engine_with(graph):
    from datahub_workflow_actions.engine import Engine
    from datahub_workflow_actions.steps import RunContext

    return Engine(RunContext(graph=graph))


def _ctx(urns):
    # (the engine owns `steps`; seed the list elsewhere in the context)
    return {"event": {"id": "e1", "operation": "COMPLETED"}, "lookup": {"urns": urns}}


def test_for_each_auto_batches_list_params(fake_graph):
    urns = [f"urn:li:dataset:{i}" for i in range(5)]
    engine = _engine_with(fake_graph)
    run = engine.run_rule(_for_each_rule({"batch": {"size": 2}}), _ctx(urns))
    step = run.steps[0]
    assert step.status == "ok" and step.reason == "5 item(s), 3 call(s)"
    assert [r.reason for r in step.items] == ["batch 1: 2 item(s)", "batch 2: 2 item(s)", "batch 3: 1 item(s)"]
    sent = [v["input"]["resources"] for _, v in fake_graph.calls]
    assert [len(s) for s in sent] == [2, 2, 1]
    assert sent[0][0]["resourceUrn"] == urns[0] and sent[2][0]["resourceUrn"] == urns[4]
    # each call has its own idempotency key
    assert len({r.idempotencyKey for r in step.items}) == 3


def test_for_each_groups_by_other_params(fake_graph):
    # tag depends on the item → items with different tags cannot share a call
    rule = _for_each_rule(params={"entity": "{{ item.urn }}", "tag": "{{ item.tag }}"})
    items = [
        {"urn": "urn:li:dataset:a", "tag": "urn:li:tag:x"},
        {"urn": "urn:li:dataset:b", "tag": "urn:li:tag:y"},
        {"urn": "urn:li:dataset:c", "tag": "urn:li:tag:x"},
    ]
    run = _engine_with(fake_graph).run_rule(rule, _ctx(items))
    assert run.steps[0].reason == "3 item(s), 2 call(s)"
    calls = [v["input"] for _, v in fake_graph.calls]
    assert calls[0]["tagUrns"] == ["urn:li:tag:x"] and [r["resourceUrn"] for r in calls[0]["resources"]] == ["urn:li:dataset:a", "urn:li:dataset:c"]
    assert calls[1]["tagUrns"] == ["urn:li:tag:y"]


def test_for_each_items_mode_runs_per_item(fake_graph):
    urns = [f"urn:li:dataset:{i}" for i in range(3)]
    run = _engine_with(fake_graph).run_rule(_for_each_rule({"batch": {"mode": "items"}}), _ctx(urns))
    assert run.steps[0].reason == "3 item(s)" and len(fake_graph.calls) == 3


def test_for_each_item_when_skips_non_matching(fake_graph):
    items = [{"urn": "urn:li:dataset:a", "type": "DATASET"}, {"urn": "urn:li:chart:b", "type": "CHART"}]
    rule = _for_each_rule(
        {"itemWhen": {"operator": "AND", "filters": [{"field": "item.type", "condition": "EQUAL", "values": ["DATASET"]}]}},
        params={"entity": "{{ item.urn }}", "tag": "urn:li:tag:pii"},
    )
    run = _engine_with(fake_graph).run_rule(rule, _ctx(items))
    step = run.steps[0]
    assert step.status == "ok" and step.reason == "2 item(s), 1 skipped"
    assert len(fake_graph.calls) == 1 and fake_graph.calls[0][1]["input"]["resources"] == [{"resourceUrn": "urn:li:dataset:a"}]
    assert [r.status for r in step.items] == ["ok", "skipped"]
    assert step.items[1].reason == "item 1: conditions not met"


def test_steps_without_bulk_param_never_batch(fake_graph):
    from datahub_workflow_actions.contract import Rule

    rule = Rule.model_validate(
        {
            "id": "r",
            "workflowUrn": "urn:li:actionWorkflow:w",
            "on": {"operation": "COMPLETED", "result": "ACCEPTED"},
            "steps": [{"id": "w", "type": "wait", "forEach": "{{ lookup.urns }}", "params": {"seconds": 0}}],
        }
    )
    run = _engine_with(fake_graph).run_rule(rule, _ctx(["a", "b"]))
    assert run.steps[0].reason == "2 item(s)" and len(run.steps[0].items) == 2
