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


# ---- §21: event trigger matching ------------------------------------------------


def _event_rule(**trigger):
    from datahub_workflow_actions.contract import Rule

    return Rule.model_validate({"id": "e", "on": {"type": "event", "category": "TAG", **trigger}, "steps": []})


def _view(**overrides):
    view = {
        "type": "EntityChangeEvent",
        "entityType": "dataset",
        "entityUrn": DATASET,
        "category": "TAG",
        "operation": "ADD",
        "modifier": "urn:li:tag:pii",
        "parameters": {"tagUrn": "urn:li:tag:pii", "context": "{}"},
        "actor": "urn:li:corpuser:jdoe",
    }
    view.update(overrides)
    return view


@pytest.mark.parametrize(
    "trigger, view, expected",
    [
        ({}, {}, None),
        ({"operations": ["ADD"], "entityTypes": ["dataset"]}, {}, None),
        ({"entityTypes": ["DATASET"]}, {}, None),  # entity types compare case-insensitively
        ({}, {"category": "OWNERSHIP"}, "category OWNERSHIP != TAG"),
        ({"operations": ["REMOVE"]}, {}, "operation ADD not in ['REMOVE']"),
        ({"entityTypes": ["schemaField"]}, {}, "entity type dataset not in ['schemaField']"),
        ({"modifier": {"values": ["urn:li:tag:pii"]}}, {}, None),
        ({"modifier": {"values": ["urn:li:tag:other"]}}, {}, "modifier urn:li:tag:pii does not match"),
        ({"modifier": {"condition": "IN", "values": ["urn:li:tag:other", "urn:li:tag:pii"]}}, {}, None),
        ({"modifier": {"condition": "START_WITH", "values": ["urn:li:tag:p"]}}, {}, None),
        ({"modifier": {"values": ["urn:li:tag:pii"], "negated": True}}, {}, "modifier urn:li:tag:pii does not match"),
        ({"modifier": {"condition": "EXISTS"}}, {"modifier": None}, "modifier None does not match"),
        ({"parameters": [{"field": "context", "condition": "EXISTS"}]}, {}, None),
        ({"parameters": [{"field": "tagUrn", "values": ["urn:li:tag:pii"]}, {"field": "context", "values": ["nope"]}]}, {}, "parameter context does not match"),
        ({"category": "DEPRECATION", "operations": ["MODIFY"], "parameters": [{"field": "status", "values": ["DEPRECATED"]}]},
         {"category": "DEPRECATION", "operation": "MODIFY", "modifier": None, "parameters": {"status": "DEPRECATED"}}, None),
        ({"category": "DEPRECATION", "operations": ["MODIFY"], "parameters": [{"field": "status", "values": ["DEPRECATED"]}]},
         {"category": "DEPRECATION", "operation": "MODIFY", "modifier": None, "parameters": {"status": "ACTIVE"}}, "parameter status does not match"),
    ],
)
def test_event_trigger_matching_table(trigger, view, expected):
    from datahub_workflow_actions.engine import event_trigger_matches

    assert event_trigger_matches(_event_rule(**trigger).on, _view(**view)) == expected


def test_event_trigger_own_actor_guard():
    from datahub_workflow_actions.engine import event_trigger_matches

    me = "urn:li:corpuser:__datahub_system"
    assert event_trigger_matches(_event_rule().on, _view(actor=me), own_actor=me) == f"own change (actor {me})"
    assert event_trigger_matches(_event_rule().on, _view(actor=me), own_actor=None) is None  # guard needs a known actor
    assert event_trigger_matches(_event_rule(ignoreOwnChanges=False).on, _view(actor=me), own_actor=me) is None
    assert event_trigger_matches(_event_rule().on, _view(), own_actor=me) is None


def test_trigger_matches_dispatches_on_trigger_type(context):
    from datahub_workflow_actions.engine import trigger_matches

    event_rule = _event_rule()
    assert trigger_matches(event_rule, {"event": _view(), "engine": {"actor": "urn:li:corpuser:x"}}) is None
    # a workflow lifecycle context is a LIFECYCLE change event — it simply is not a TAG event
    assert trigger_matches(event_rule, context) == "category LIFECYCLE != TAG"
    # a workflow rule against an event context still fails on the workflow gate
    from datahub_workflow_actions.contract import Rule

    assert trigger_matches(Rule.model_validate(rule()), {"event": _view()}) == "event has no workflow urn"


# ---------------------------------------------------------------------------
# §22 branches
# ---------------------------------------------------------------------------


def _branch(cond_value, then, else_, **extra):
    return {"id": extra.pop("id", "b"), "type": "branch", "if": {"operator": "AND", "filters": [{"field": "entity.type", "values": [cond_value]}]}, "then": then, "else": else_, **extra}


def _rec(step_id, value=None):
    return {"id": step_id, "type": "_record", "params": {"value": value or step_id}}


def test_branch_runs_the_matching_lane_records_the_other_as_skipped_and_rejoins(context):
    CALLS.clear()
    config = cfg(rule(steps=[_branch("dataset", [_rec("yes")], [_rec("no")]), _rec("after", "{{ steps.no.status }}")]))
    run = Engine().run(config, context)[0]
    assert [(s.stepId, s.status) for s in run.steps] == [("b", "ok"), ("no", "skipped"), ("yes", "ok"), ("after", "ok")]
    branch = run.steps[0]
    assert branch.type == "branch" and branch.output == {"taken": "then", "matched": True} and "Matches lane" in branch.reason
    assert run.steps[1].reason == "branch 'b' took then"
    # the dead lane's step is addressable from later templates
    assert CALLS == ["yes", "skipped"] and run.status == "ok"


def test_branch_takes_else_when_the_condition_fails(context):
    CALLS.clear()
    run = Engine().run(cfg(rule(steps=[_branch("chart", [_rec("yes")], [_rec("no")])])), context)[0]
    assert [(s.stepId, s.status) for s in run.steps] == [("b", "ok"), ("yes", "skipped"), ("no", "ok")]
    assert run.steps[0].output["taken"] == "else" and "Otherwise lane" in run.steps[0].reason and CALLS == ["no"]


def test_nested_branches_and_dead_lanes_skip_their_whole_subtree(context):
    CALLS.clear()
    inner = _branch("dataset", [_rec("deep")], [_rec("deeper")], id="inner")
    run = Engine().run(cfg(rule(steps=[_branch("chart", [inner, _rec("y")], [_rec("n")]), _rec("end")])), context)[0]
    statuses = {s.stepId: s.status for s in run.steps}
    assert statuses == {"b": "ok", "inner": "skipped", "deep": "skipped", "deeper": "skipped", "y": "skipped", "n": "ok", "end": "ok"}
    assert CALLS == ["n", "end"]
    # and the nested branch runs when its lane is taken
    CALLS.clear()
    run = Engine().run(cfg(rule(steps=[_branch("dataset", [inner, _rec("y")], [_rec("n")])])), context)[0]
    assert [s.stepId for s in run.steps if s.status == "ok"] == ["b", "inner", "deep", "y"] and CALLS == ["deep", "y"]


def test_failures_inside_a_lane_follow_the_error_policy(context):
    for policy, expected_status, ran_after in (("fail", "failed", False), ("stop", "stopped", False), ("continue", "ok", True)):
        CALLS.clear()
        failing = {"id": "boom", "type": "_record", "params": {"value": "boom", "fail_times": 5}}
        run = Engine().run(cfg(rule(onError=policy, steps=[_branch("dataset", [failing, _rec("in-lane")], [_rec("no")]), _rec("after")])), context)[0]
        assert run.status == expected_status, policy
        assert ("after" in CALLS) is ran_after, policy
        assert ("in-lane" in CALLS) is ran_after, policy
        if not ran_after:
            assert run.reason.startswith("step 'boom' failed") and [s.stepId for s in run.steps] == ["b", "no", "boom"]


def test_disabled_or_gated_branch_skips_both_lanes(context):
    CALLS.clear()
    gated = _branch("dataset", [_rec("yes")], [_rec("no")], when={"operator": "AND", "filters": [{"field": "entity.type", "values": ["chart"]}]})
    run = Engine().run(cfg(rule(steps=[gated, _rec("after")])), context)[0]
    assert [(s.stepId, s.status, s.reason) for s in run.steps][:3] == [("b", "skipped", "conditions not met"), ("yes", "skipped", "branch 'b' skipped"), ("no", "skipped", "branch 'b' skipped")]
    assert CALLS == ["after"]
    CALLS.clear()
    run = Engine().run(cfg(rule(steps=[_branch("dataset", [_rec("yes")], [], enabled=False)])), context)[0]
    assert run.steps[0].reason == "disabled" and CALLS == []


def test_branch_is_idempotent_on_replay_and_dry_run(context):
    CALLS.clear()
    engine = Engine()
    config = cfg(rule(steps=[_branch("dataset", [_rec("yes")], [])]))
    engine.run(config, context)
    replay = engine.run(config, context)[0]
    assert replay.steps[0].status == "ok"  # the branch row itself never reports "already ran"
    assert replay.steps[1].status == "skipped" and "idempotency" in replay.steps[1].reason
    dry = Engine(dry_run=True).run(config, context)[0]
    assert dry.steps[0].status == "dry-run" and dry.steps[0].output["taken"] == "then" and dry.steps[1].status == "dry-run"
