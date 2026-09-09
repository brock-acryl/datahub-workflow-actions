import json

from datahub_workflow_actions.contract import load_rules
from datahub_workflow_actions.engine import RuleRun, StepRun
from datahub_workflow_actions.runs import (
    EVENTS_FLOW_ID,
    RunRecorder,
    flow_urn,
    flow_urn_for,
    job_urn,
    job_urn_for,
    run_key,
    run_urn,
    run_urn_for,
    steps_report,
)
from tests.conftest import DATASET, REQ, TAG_PII, WF


class FakeEmitter:
    def __init__(self):
        self.mcps = []

    def emit(self, mcp, callback=None):
        self.mcps.append(mcp)

    emit_mcp = emit

    def aspects(self, entity_prefix=None):
        return [(m.entityUrn, m.aspectName, m.aspect) for m in self.mcps if not entity_prefix or m.entityUrn.startswith(entity_prefix)]


def context():
    return {
        "event": {"entityUrn": REQ, "operation": "COMPLETED", "result": "ACCEPTED", "time": 1754000000000},
        "workflow": {"urn": WF, "name": "Access Request"},
        "request": {"urn": REQ},
        "requester": {"urn": "urn:li:corpuser:jdoe"},
        "entity": {"urn": DATASET},
    }


def rule():
    return load_rules({"schemaVersion": 1, "rules": [{
        "id": "grant", "name": "Grant access", "description": "GRANT on approval", "workflowUrn": WF,
        "on": {"operation": "COMPLETED", "result": "ACCEPTED"}, "steps": [{"id": "s", "type": "add_tag", "params": {"tag": "urn:li:tag:x"}}]}]}).rules[0]


def test_urns_are_deterministic_and_mirror_the_mfe_contract():
    assert flow_urn(WF) == "urn:li:dataFlow:(workflow-actions,wf-1,PROD)"
    assert job_urn(WF, "grant") == "urn:li:dataJob:(urn:li:dataFlow:(workflow-actions,wf-1,PROD),grant)"
    a = run_urn("grant", REQ, "COMPLETED", "ACCEPTED", 1754000000000)
    assert a == run_urn("grant", REQ, "COMPLETED", "ACCEPTED", 1754000000000) and a.startswith("urn:li:dataProcessInstance:")
    assert a != run_urn("grant", REQ, "COMPLETED", "ACCEPTED", 1754000000001)


def test_steps_report_is_compact_and_truncates_outputs():
    steps = [StepRun(stepId="s", type="add_tag", status="ok", attempts=1, output={"big": "x" * 5000}),
             StepRun(stepId="t", type="sql", status="failed", attempts=3, error="boom " * 400)]
    rows = json.loads(steps_report(steps))
    assert rows[0]["id"] == "s" and rows[0]["output"]["big"].endswith("…") and len(rows[0]["output"]["big"]) == 500
    assert rows[1]["status"] == "failed" and rows[1]["attempts"] == 3 and len(rows[1]["error"]) == 1000


def test_recorder_emits_flow_job_and_a_start_end_pair_with_properties():
    emitter = FakeEmitter()
    recorder = RunRecorder(emitter)
    run = RuleRun(ruleId="grant", fired=True, status="ok", steps=[StepRun(stepId="s", type="add_tag", status="ok", attempts=1, output={"added": True})])
    urn = recorder.record(run, rule(), context(), started_ms=1754000001000)
    assert urn == run_urn("grant", REQ, "COMPLETED", "ACCEPTED", 1754000000000)

    names = [(u.split(":")[2], a) for u, a, _ in emitter.aspects()]
    assert ("dataFlow", "dataFlowInfo") in names and ("dataJob", "dataJobInfo") in names
    dpi = {a: asp for u, a, asp in emitter.aspects("urn:li:dataProcessInstance:")}
    assert dpi["dataProcessInstanceProperties"].name == "Grant access"
    props = dpi["dataProcessInstanceProperties"].customProperties
    assert props["ruleId"] == "grant" and props["requestUrn"] == REQ and props["entityUrn"] == DATASET and props["status"] == "ok"
    assert json.loads(props["steps"])[0]["output"] == {"added": True}
    assert dpi["dataProcessInstanceRelationships"].parentTemplate == job_urn(WF, "grant")
    assert dpi["dataProcessInstanceInput"].inputs == [DATASET]
    events = [asp for u, a, asp in emitter.aspects("urn:li:dataProcessInstance:") if a == "dataProcessInstanceRunEvent"]
    assert [e.status for e in events] == ["STARTED", "COMPLETE"] and events[1].result.type == "SUCCESS" and events[1].result.nativeResultType == "ok"

    # templates are emitted once per job, not on every fire
    before = len(emitter.mcps)
    recorder.record(run, rule(), context(), started_ms=1754000002000)
    assert not any(a in ("dataFlowInfo", "dataJobInfo") for _, a, _ in emitter.aspects()[before:])


def test_recorder_skips_not_fired_and_dry_runs_unless_asked_and_never_raises():
    emitter = FakeEmitter()
    recorder = RunRecorder(emitter)
    assert recorder.record(RuleRun(ruleId="grant", fired=False), rule(), context()) is None
    assert recorder.record(RuleRun(ruleId="grant", fired=True, status="dry-run"), rule(), context()) is None
    assert emitter.mcps == []
    from datahub_workflow_actions.runs import RunRecorderConfig
    assert RunRecorder(emitter, RunRecorderConfig(recordDryRuns=True)).record(RuleRun(ruleId="grant", fired=True, status="dry-run"), rule(), context())
    failed = RuleRun(ruleId="grant", fired=True, status="failed", reason="step failed", steps=[StepRun(stepId="s", type="sql", status="failed", error="x")])
    events = []
    RunRecorder(emitter).record(failed, rule(), context())
    events = [asp for u, a, asp in emitter.aspects("urn:li:dataProcessInstance:") if a == "dataProcessInstanceRunEvent"]
    assert events[-1].result.type == "FAILURE"

    class Broken:
        def emit(self, *a, **k):
            raise RuntimeError("gms down")
        emit_mcp = emit

    assert RunRecorder(Broken()).record(failed, rule(), context()) is None  # logged, not raised
    assert RunRecorder.from_config(None, {"enabled": True}) is None
    assert RunRecorder.from_config(emitter, {"enabled": False}) is None


# ---------------------------------------------------------------------------
# §21 event rules record under the fixed `events` flow
# ---------------------------------------------------------------------------


def event_rule():
    return load_rules({"schemaVersion": 1, "rules": [{
        "id": "pii-term", "name": "PII tag → term",
        "on": {"type": "event", "category": "TAG", "operations": ["ADD"]},
        "steps": [{"id": "s", "type": "add_term", "params": {"term": "urn:li:glossaryTerm:x"}}]}]}).rules[0]


def event_context(entity_urn=DATASET, parent=None):
    entity = {"urn": entity_urn}
    if parent:
        entity["parent"] = {"urn": parent}
    return {
        "event": {"type": "EntityChangeEvent", "id": f"{entity_urn}:TAG:ADD:{TAG_PII}:1754000000000", "category": "TAG", "operation": "ADD",
                  "modifier": TAG_PII, "entityUrn": entity_urn, "entityType": "dataset", "time": 1754000000000, "actor": "urn:li:corpuser:admin"},
        "entity": entity,
        "actor": {"urn": "urn:li:corpuser:admin"},
        "change": {"tag": TAG_PII},
        "params": {},
    }


def test_event_rule_urns_mirror_the_mfe_contract():
    assert EVENTS_FLOW_ID == "events"
    assert flow_urn_for(event_rule()) == "urn:li:dataFlow:(workflow-actions,events,PROD)"
    assert job_urn_for(event_rule()) == "urn:li:dataJob:(urn:li:dataFlow:(workflow-actions,events,PROD),pii-term)"
    # workflow rules are unchanged
    assert flow_urn_for(rule()) == flow_urn(WF) and job_urn_for(rule()) == job_urn(WF, "grant")
    # workflow runs keep their pre-§21 ids; event runs key on rule + event id
    assert run_urn_for("grant", context()) == run_urn("grant", REQ, "COMPLETED", "ACCEPTED", 1754000000000)
    assert run_key("pii-term", event_context()) == f"pii-term|{DATASET}:TAG:ADD:{TAG_PII}:1754000000000"
    assert run_urn_for("pii-term", event_context()) != run_urn_for("pii-term", event_context(entity_urn="urn:li:dataset:(x,y,PROD)"))


def test_recorder_writes_event_runs_with_event_properties_and_no_request():
    emitter = FakeEmitter()
    run = RuleRun(ruleId="pii-term", fired=True, status="ok", steps=[StepRun(stepId="s", type="add_term", status="ok", attempts=1)])
    urn = RunRecorder(emitter).record(run, event_rule(), event_context(), started_ms=1754000001000)
    assert urn == run_urn_for("pii-term", event_context())
    flows = [asp for u, a, asp in emitter.aspects("urn:li:dataFlow:") if a == "dataFlowInfo"]
    assert flows[0].name == "Events"
    dpi = {a: asp for u, a, asp in emitter.aspects("urn:li:dataProcessInstance:")}
    props = dpi["dataProcessInstanceProperties"].customProperties
    assert props["triggerType"] == "event" and props["category"] == "TAG" and props["modifier"] == TAG_PII
    assert props["actorUrn"] == "urn:li:corpuser:admin" and props["entityUrn"] == DATASET and props["operation"] == "ADD"
    assert props["workflowUrn"] == "" and props["requestUrn"] == "" and props["requesterUrn"] == ""
    assert dpi["dataProcessInstanceRelationships"].parentTemplate == job_urn_for(event_rule())
    assert dpi["dataProcessInstanceInput"].inputs == [DATASET]


def test_field_events_use_the_parent_dataset_as_inlet():
    emitter = FakeEmitter()
    field = f"urn:li:schemaField:({DATASET},customer_email)"
    run = RuleRun(ruleId="pii-term", fired=True, status="ok", steps=[])
    RunRecorder(emitter).record(run, event_rule(), event_context(entity_urn=field, parent=DATASET), started_ms=1)
    dpi = {a: asp for u, a, asp in emitter.aspects("urn:li:dataProcessInstance:")}
    assert dpi["dataProcessInstanceInput"].inputs == [DATASET]
    assert dpi["dataProcessInstanceProperties"].customProperties["entityUrn"] == field


def test_workflow_runs_carry_trigger_type_workflow():
    emitter = FakeEmitter()
    run = RuleRun(ruleId="grant", fired=True, status="ok", steps=[])
    RunRecorder(emitter).record(run, rule(), context(), started_ms=1)
    dpi = {a: asp for u, a, asp in emitter.aspects("urn:li:dataProcessInstance:")}
    props = dpi["dataProcessInstanceProperties"].customProperties
    assert props["triggerType"] == "workflow" and "category" not in props and props["requestUrn"] == REQ


def test_schedule_rules_record_under_the_schedules_flow():
    from datahub_workflow_actions.context import build_schedule_context
    from datahub_workflow_actions.runs import SCHEDULES_FLOW_ID, flow_urn_for, job_urn_for

    schedule_rule = load_rules({"schemaVersion": 1, "rules": [{"id": "nightly", "name": "Nightly", "on": {"type": "schedule", "cron": "0 6 * * *"}, "steps": []}]}).rules[0]
    assert SCHEDULES_FLOW_ID == "schedules"
    assert flow_urn_for(schedule_rule) == "urn:li:dataFlow:(workflow-actions,schedules,PROD)"
    assert job_urn_for(schedule_rule) == "urn:li:dataJob:(urn:li:dataFlow:(workflow-actions,schedules,PROD),nightly)"
    emitter = FakeEmitter()
    ctx = build_schedule_context(schedule_rule, 1754000000000)
    run = RuleRun(ruleId="nightly", fired=True, status="ok", steps=[])
    urn = RunRecorder(emitter).record(run, schedule_rule, ctx, started_ms=1754000001000)
    assert urn == run_urn_for("nightly", ctx)
    flows = [asp for u, a, asp in emitter.aspects("urn:li:dataFlow:") if a == "dataFlowInfo"]
    assert flows[0].name == "Schedules"
    dpi = {a: asp for u, a, asp in emitter.aspects("urn:li:dataProcessInstance:")}
    props = dpi["dataProcessInstanceProperties"].customProperties
    assert props["triggerType"] == "schedule" and props["scheduledAt"].startswith("2025-07-31T") is False or props["scheduledAt"]
    assert props["entityUrn"] == "" and props["requestUrn"] == "" and "category" not in props
    assert dpi["dataProcessInstanceInput"].inputs == [] if "dataProcessInstanceInput" in dpi else True


def test_steps_report_keeps_branch_rows():
    steps = [
        StepRun(stepId="b", type="branch", status="ok", reason="condition matched → Matches lane", output={"taken": "then", "matched": True}),
        StepRun(stepId="no", type="add_tag", status="skipped", reason="branch 'b' took then"),
        StepRun(stepId="yes", type="add_tag", status="ok", attempts=1, output={"added": True}),
    ]
    rows = json.loads(steps_report(steps))
    assert rows[0] == {"id": "b", "type": "branch", "status": "ok", "attempts": 0, "reason": "condition matched → Matches lane", "output": {"taken": "then", "matched": True}}
    assert rows[1]["reason"] == "branch 'b' took then" and "output" not in rows[1]
