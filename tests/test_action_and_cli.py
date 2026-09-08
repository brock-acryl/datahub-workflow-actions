import json

import pytest

from datahub_workflow_actions import cli
from datahub_workflow_actions.action import WorkflowActionsAction
from tests.conftest import ADMIN, DATASET, FIXTURES, TAG_PII, WF, completed_event, deprecation_event, field_tag_added_event, tag_added_event

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
    # §21: every change event reaches the action; the EventIndex does the narrowing
    assert pipeline["filter"] == {"event_type": "EntityChangeEvent_v1"}


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


def test_cli_triggers_and_event_rule_validation(tmp_path, capsys):
    assert cli.main(["triggers"]) == 0
    out = json.loads(capsys.readouterr().out)
    assert out["categories"]["TAG"]["operations"] == ["ADD", "REMOVE"] and "schemaField" in out["entityTypes"]

    import pathlib

    example = pathlib.Path(__file__).resolve().parents[1] / "examples" / "event-rules.yaml"
    assert cli.main(["validate", str(example)]) == 0
    captured = capsys.readouterr()
    assert "3 rule(s) — 0 workflow, 3 event" in captured.out and "WARNING" not in captured.err

    odd = tmp_path / "odd.json"
    odd.write_text(json.dumps({"schemaVersion": 1, "rules": [
        {"id": "x", "on": {"type": "event", "category": "TAG", "operations": ["MODIFY"], "entityTypes": ["spaceship"]}, "steps": []},
        {"id": "y", "on": {"type": "event", "category": "MYSTERY"}, "steps": []},
    ]}))
    assert cli.main(["validate", str(odd)]) == 0  # advice only
    err = capsys.readouterr().err
    assert "not known to emit operation 'MODIFY'" in err and "unknown entity type 'spaceship'" in err and "category 'MYSTERY'" in err


# ---------------------------------------------------------------------------
# §21 event rules through the action
# ---------------------------------------------------------------------------

EVENT_RULES = {
    "schemaVersion": 1,
    "rules": [
        RULES["rules"][0],  # the workflow rule stays
        {
            "id": "pii-term",
            "name": "PII tag → term",
            "on": {"type": "event", "category": "TAG", "operations": ["ADD"], "entityTypes": ["dataset"], "modifier": {"values": [TAG_PII]}},
            "steps": [{"id": "term", "type": "add_term", "params": {"entity": "{{ entity.urn }}", "term": "urn:li:glossaryTerm:Sensitive"}}],
        },
        {
            "id": "deprecated-webhook",
            "on": {"type": "event", "category": "DEPRECATION", "parameters": [{"field": "status", "values": ["DEPRECATED"]}]},
            "steps": [{"id": "hook", "type": "webhook", "params": {"url": "http://hooks.invalid/x", "body": "{{ change.note }}"}}],
        },
        {
            "id": "own-changes-too",
            "on": {"type": "event", "category": "OWNERSHIP", "ignoreOwnChanges": False},
            "steps": [{"id": "tag", "type": "add_tag", "params": {"entity": "{{ entity.urn }}", "tag": "urn:li:tag:owned"}}],
        },
    ],
}


def _action(fake_graph, **config):
    class G:
        graph = fake_graph

    class C:
        graph = G()

    return WorkflowActionsAction.create({**EVENT_RULES, "inMemoryState": True, "ownActor": "urn:li:corpuser:__datahub_system", **config}, C())


def _mutations(fake_graph):
    return [q.split("{", 1)[1].split("(", 1)[0].strip() for q, _ in fake_graph.calls if q.lstrip().startswith("mutation")]


def test_tag_add_fires_only_the_matching_event_rule(fake_graph):
    action = _action(fake_graph)
    assert action.index.describe() == "DEPRECATION:1, OWNERSHIP:1, TAG:1"
    runs = action.handle_event(tag_added_event())
    assert [r.ruleId for r in runs] == ["pii-term"]  # the workflow rule and the other categories are never even evaluated
    assert runs[0].fired and runs[0].status == "ok"
    assert _mutations(fake_graph) == ["batchAddTerms"]
    # redelivery is idempotent
    fake_graph.calls.clear()
    runs2 = action.handle_event(tag_added_event())
    assert runs2[0].steps[0].status == "skipped" and _mutations(fake_graph) == []


def test_non_candidate_events_cost_no_graphql(fake_graph):
    action = _action(fake_graph)
    assert action.handle_event(tag_added_event(tag="urn:li:tag:other")) == []  # modifier mismatch
    assert action.handle_event(tag_added_event(operation="REMOVE")) == []  # operation mismatch
    assert action.handle_event({**tag_added_event(), "category": "GLOSSARY_TERM"}) == []  # no bucket
    assert action.handle_event(field_tag_added_event()) == []  # entityTypes: dataset only
    assert fake_graph.calls == []


def test_own_changes_are_skipped_unless_the_rule_opts_in(fake_graph):
    action = _action(fake_graph)
    own = "urn:li:corpuser:__datahub_system"
    assert action.handle_event(tag_added_event(actor=own)) == []
    owner_event = {**tag_added_event(actor=own), "category": "OWNERSHIP", "modifier": "urn:li:corpuser:x", "parameters": {"ownerUrn": "urn:li:corpuser:x"}}
    runs = action.handle_event(owner_event)
    assert [r.ruleId for r in runs] == ["own-changes-too"] and runs[0].fired


def test_own_actor_is_resolved_once_from_me(fake_graph):
    class G:
        graph = fake_graph

    class C:
        graph = G()

    fake_graph.me = {"me": {"corpUser": {"urn": "urn:li:corpuser:engine"}}}
    original = fake_graph.execute_graphql

    def execute(query, variables=None, **kw):
        if "me {" in query:
            fake_graph.calls.append((query, variables))
            return fake_graph.me
        return original(query, variables, **kw)

    fake_graph.execute_graphql = execute
    action = WorkflowActionsAction.create({**EVENT_RULES, "inMemoryState": True}, C())
    assert action.own_actor() == "urn:li:corpuser:engine"
    assert action.own_actor() == "urn:li:corpuser:engine"
    assert sum(1 for q, _ in fake_graph.calls if "me {" in q) == 1
    assert action.handle_event(tag_added_event(actor="urn:li:corpuser:engine")) == []


def test_parameter_filters_and_change_vocabulary(fake_graph, monkeypatch):
    sent = {}

    class FakeResponse:
        status_code = 200
        text = "ok"

        def json(self):
            return {"ok": True}

    def fake_request(method, url, **kw):
        sent.update(method=method, url=url, body=kw.get("data") or kw.get("json"))
        return FakeResponse()

    try:
        import requests

        monkeypatch.setattr(requests, "request", fake_request, raising=False)
        monkeypatch.setattr(requests, "post", lambda url, **kw: fake_request("POST", url, **kw), raising=False)
    except ImportError:
        pass
    action = _action(fake_graph)
    runs = action.handle_event(deprecation_event())
    assert [r.ruleId for r in runs] == ["deprecated-webhook"] and runs[0].fired
    assert action.handle_event(deprecation_event(status="ACTIVE")) == []


def test_workflow_events_still_take_the_workflow_path(fake_graph):
    action = _action(fake_graph)
    runs = action.handle_event(completed_event())
    assert [r.ruleId for r in runs if r.fired] == ["on-approval"]
    assert _mutations(fake_graph) == ["batchAddTags"]


def test_hot_reload_rebuilds_the_event_index(fake_graph):
    action = _action(fake_graph)
    assert len(action.index) == 3
    action.apply_config({"schemaVersion": 1, "rules": [EVENT_RULES["rules"][1]]})
    assert len(action.index) == 1 and action.index.describe() == "TAG:1"
    assert action.handle_event(deprecation_event()) == []
    assert [r.ruleId for r in action.handle_event(tag_added_event())] == ["pii-term"]


def test_cli_simulate_event_file_with_own_actor(tmp_path, capsys):
    rules = tmp_path / "rules.json"
    rules.write_text(json.dumps(EVENT_RULES))
    event = tmp_path / "event.json"
    event.write_text(json.dumps(tag_added_event()))
    assert cli.main(["simulate", "--rules", str(rules), "--event", str(event), "--show-context"]) == 0
    out = json.loads(capsys.readouterr().out)
    assert "workflow" not in out["context"] and out["context"]["change"]["tag"] == TAG_PII
    by_id = {r["ruleId"]: r for r in out["runs"]}
    assert by_id["pii-term"]["fired"] and by_id["pii-term"]["status"] == "dry-run"
    assert by_id["on-approval"]["fired"] is False and by_id["deprecated-webhook"]["fired"] is False
    # the same event by the engine's own actor is skipped
    assert cli.main(["simulate", "--rules", str(rules), "--event", str(event), "--own-actor", ADMIN]) == 0
    out = json.loads(capsys.readouterr().out)
    pii = next(r for r in out["runs"] if r["ruleId"] == "pii-term")
    assert pii["fired"] is False and "own change" in pii["reason"]


def test_example_event_rules_and_sample_events_stay_in_step(capsys):
    """examples/ are documentation: every sample event must be evaluated cleanly (dry run) and the
    ones that describe a rule must fire it."""
    from pathlib import Path

    root = Path(__file__).resolve().parents[1]
    rules = str(root / "examples" / "event-rules.yaml")
    fixtures = str(root / "examples" / "events" / "fixtures.json")
    assert cli.main(["validate", rules]) == 0
    capsys.readouterr()
    expected = {"tag_added.json": ["pii-tag-adds-steward"], "field_tag_added.json": ["pii-tag-adds-steward"], "deprecated.json": ["deprecation-notifies-owners"], "owner_removed.json": [], "assertion_failed.json": []}
    for name, fired in expected.items():
        code = cli.main(["simulate", "--rules", rules, "--event", str(root / "examples" / "events" / name), "--fixtures", fixtures])
        out = json.loads(capsys.readouterr().out)
        assert code == 0, (name, out)
        assert [r["ruleId"] for r in out["runs"] if r["fired"]] == fired, name
        assert all(r["status"] in ("dry-run", "not-fired") for r in out["runs"]), (name, out)
