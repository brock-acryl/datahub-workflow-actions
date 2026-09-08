import json

import pytest

from datahub_workflow_actions import cli
from datahub_workflow_actions.action import WorkflowActionsAction
from tests.conftest import DATASET, FIXTURES, WF, completed_event

datahub_actions = pytest.importorskip("datahub_actions")
from datahub_actions.event.event_envelope import EventEnvelope  # noqa: E402
from datahub_actions.event.event_registry import ENTITY_CHANGE_EVENT_V1_TYPE, EntityChangeEvent  # noqa: E402

RULES = {
    "schemaVersion": 1,
    "rules": [
        {
            "id": "on-approval",
            "workflowUrn": WF,
            "on": {"operation": "COMPLETED", "result": "ACCEPTED"},
            "when": {"operator": "AND", "filters": [{"field": "form.field_abc", "values": ["restricted"]}]},
            "steps": [
                {"id": "tag", "type": "add_tag", "params": {"entity": "{{ entity.urn }}", "tag": "urn:li:tag:access-granted"}},
                {"id": "owner", "type": "add_owner", "forEach": "{{ entity.owners }}", "params": {"entity": "{{ entity.urn }}", "owner": "{{ item }}"}},
            ],
        },
        {"id": "other-wf", "workflowUrn": "urn:li:actionWorkflow:other", "on": {"operation": "CREATE"}, "steps": [{"id": "x", "type": "wait", "params": {"seconds": 1}}]},
    ],
}


class Ctx:
    class graph:  # mimics AcrylDataHubGraph.graph
        graph = None


def test_action_create_and_act_dry_run(fake_graph):
    class G:
        graph = fake_graph

    class C:
        graph = G()

    action = WorkflowActionsAction.create({**RULES, "dryRun": True, "inMemoryState": True}, C())
    ece = EntityChangeEvent.from_obj(completed_event())
    action.act(EventEnvelope(ENTITY_CHANGE_EVENT_V1_TYPE, ece, {}))
    # dry run → context is still resolved (reads), but no mutation reaches the graph
    assert fake_graph.calls and not any(q.lstrip().startswith("mutation") for q, _ in fake_graph.calls)
    # ignores non-ECE envelopes
    action.act(EventEnvelope("MetadataChangeLogEvent_v1", ece, {}))


def test_action_real_run_calls_graph(fake_graph):
    class G:
        graph = fake_graph

    class C:
        graph = G()

    action = WorkflowActionsAction.create({**RULES, "inMemoryState": True}, C())
    runs = action.handle_event(completed_event())
    fired = [r for r in runs if r.fired]
    assert [r.ruleId for r in fired] == ["on-approval"]
    # tag mutation + entity resolution queries; add_owner fanned out over resolved owners (none without fixtures → 0 items)
    mutations = [q.split("{", 1)[1].split("(", 1)[0].strip() for q, _ in fake_graph.calls if q.lstrip().startswith("mutation")]
    assert mutations == ["batchAddTags"]
    # second delivery is idempotent
    runs2 = action.handle_event(completed_event())
    assert [s.status for s in runs2[0].steps][0] == "skipped"


def test_cli_schema_catalog_validate_simulate(tmp_path, capsys):
    rules = tmp_path / "rules.json"
    rules.write_text(json.dumps(RULES))
    event = tmp_path / "event.json"
    event.write_text(json.dumps(completed_event()))
    fixtures = tmp_path / "fixtures.json"
    fixtures.write_text(json.dumps(FIXTURES))

    assert cli.main(["schema"]) == 0
    assert json.loads(capsys.readouterr().out)["$defs"]["Rule"]
    assert cli.main(["catalog"]) == 0
    assert any(c["type"] == "jira_issue" for c in json.loads(capsys.readouterr().out))
    assert cli.main(["validate", str(rules)]) == 0
    assert "OK: 2 rule(s)" in capsys.readouterr().out

    bad = tmp_path / "bad.json"
    bad.write_text(json.dumps({"rules": [{"id": "r", "workflowUrn": WF, "on": {"operation": "CREATE"}, "steps": [{"id": "s", "type": "nope"}]}]}))
    assert cli.main(["validate", str(bad)]) == 1

    assert cli.main(["simulate", "--rules", str(rules), "--event", str(event), "--fixtures", str(fixtures), "--show-context"]) == 0
    out = json.loads(capsys.readouterr().out)
    run = next(r for r in out["runs"] if r["ruleId"] == "on-approval")
    assert run["status"] == "dry-run"
    assert run["steps"][0]["output"]["variables"]["input"]["tagUrns"] == ["urn:li:tag:access-granted"]
    assert run["steps"][1]["reason"] == "2 item(s), 2 call(s)"  # fanned out over the two fixture owners (distinct owners → no merge)
    assert run["steps"][1]["items"][1]["params"]["owner"] == "urn:li:corpGroup:data-eng"
    assert out["context"]["entity"]["urn"] == DATASET
    assert next(r for r in out["runs"] if r["ruleId"] == "other-wf")["reason"].startswith("workflow")


def test_source_builds_actions_pipeline_config():
    from datahub_workflow_actions.source import WorkflowActionsSourceConfig, WorkflowActionsSource

    cfg = WorkflowActionsSourceConfig.model_validate({**RULES, "kafka": {"connection": {"bootstrap": "kafka:9092"}}, "statePath": "/tmp/x.db"})
    src = WorkflowActionsSource.__new__(WorkflowActionsSource)
    src.config = cfg
    from datahub_workflow_actions.contract import load_rules

    src.rules = load_rules(RULES)
    pipeline = src.actions_pipeline_config()
    assert pipeline["source"]["type"] == "kafka"
    assert pipeline["source"]["config"]["connection"]["bootstrap"] == "kafka:9092"
    assert pipeline["source"]["config"]["connection"]["consumer_config"]["max.poll.interval.ms"] == "900000"
    assert pipeline["action"]["type"] == "workflow_actions" and len(pipeline["action"]["config"]["rules"]) == 2
    assert pipeline["filter"]["event"]["entityType"] == "actionRequest"


def test_act_keeps_event_parameters_from_the_actions_envelope(fake_graph, monkeypatch):
    """datahub-actions stores EntityChangeEvent parameters outside the Avro record;
    to_obj() drops them, which made every rule report "event has no workflow urn"."""
    import json

    from datahub_workflow_actions.action import event_payload

    raw = {
        "entityType": "actionRequest",
        "entityUrn": "urn:li:actionRequest:r1",
        "category": "LIFECYCLE",
        "operation": "COMPLETED",
        "auditStamp": {"time": 1, "actor": "urn:li:corpuser:admin"},
        "version": 0,
        "parameters": {"workflowUrn": WF, "result": "ACCEPTED", "actionRequestType": "WORKFLOW_FORM_REQUEST", "entityUrn": DATASET},
    }
    event = EntityChangeEvent.from_json(json.dumps(raw))
    assert "parameters" not in event.to_obj()  # the datahub-actions quirk this guards against
    payload = event_payload(event)
    assert payload["parameters"]["workflowUrn"] == WF and payload["operation"] == "COMPLETED"

    class G:
        graph = fake_graph

    class C:
        graph = G()

    action = WorkflowActionsAction.create({**RULES, "inMemoryState": True, "dryRun": True}, C())
    seen = {}
    monkeypatch.setattr(action, "handle_event", lambda p: seen.setdefault("payload", p) or [])
    action.act(EventEnvelope(ENTITY_CHANGE_EVENT_V1_TYPE, event, {}))
    assert seen["payload"]["parameters"]["workflowUrn"] == WF
