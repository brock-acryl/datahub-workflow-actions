import json

from datahub_workflow_actions.contract import load_rules
from datahub_workflow_actions.engine import RuleRun, StepRun
from datahub_workflow_actions.runs import RunRecorder, flow_urn, job_urn, run_urn, steps_report
from tests.conftest import DATASET, REQ, WF


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
