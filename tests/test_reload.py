"""Hot reload (§18.24): the watcher swaps rules between events, keeps the last good
config on bad input, and the action keeps its idempotency state across a reload."""

import json

from datahub_workflow_actions.action import WorkflowActionsAction
from datahub_workflow_actions.reload import RecipeWatcher, fetch_source_config, reloadable_fingerprint, source_config_of
from tests.conftest import DATASET, WF, FakeGraph


def rule(rule_id, tag="urn:li:tag:a", enabled=True):
    return {
        "id": rule_id,
        "workflowUrn": WF,
        "enabled": enabled,
        "on": {"operation": "COMPLETED", "result": "ACCEPTED"},
        "steps": [{"id": "t", "type": "add_tag", "params": {"entity": "{{ entity.urn }}", "tag": tag}}],
    }


def config(*rules, **extra):
    return {"schemaVersion": 1, "rules": list(rules), **extra}


class RecipeGraph(FakeGraph):
    def __init__(self, recipe_text):
        super().__init__()
        self.recipe_text = recipe_text

    def execute_graphql(self, query, variables=None, **_):
        if "ingestionSource(urn: $urn)" in query:
            return {"ingestionSource": {"urn": variables["urn"], "config": {"recipe": self.recipe_text}}}
        return super().execute_graphql(query, variables)


def test_fingerprint_ignores_settings_that_need_a_restart():
    a = config(rule("r1"), kafka={"connection": {"bootstrap": "a"}}, executorId="x", statePath="/a.db")
    b = config(rule("r1"), kafka={"connection": {"bootstrap": "b"}}, executorId="y", statePath="/b.db")
    assert reloadable_fingerprint(a) == reloadable_fingerprint(b)
    assert reloadable_fingerprint(config(rule("r1"))) != reloadable_fingerprint(config(rule("r1", enabled=False)))
    assert reloadable_fingerprint(config(rule("r1"))) != reloadable_fingerprint(config(rule("r1"), connections={"w": {"url": "x"}}))


def test_fetch_reads_json_or_yaml_recipes():
    recipe = {"source": {"type": "datahub-workflow-actions", "config": config(rule("r1"))}}
    assert fetch_source_config(RecipeGraph(json.dumps(recipe)), "urn:li:dataHubIngestionSource:s")["rules"][0]["id"] == "r1"
    yaml_text = "source:\n  type: datahub-workflow-actions\n  config:\n    schemaVersion: 1\n    rules: []\n"
    assert fetch_source_config(RecipeGraph(yaml_text), "urn:li:dataHubIngestionSource:s") == {"schemaVersion": 1, "rules": []}
    assert fetch_source_config(RecipeGraph(""), "urn:li:dataHubIngestionSource:s") is None
    assert source_config_of({"source": "nope"}) == {}


def test_watcher_applies_only_real_changes_and_keeps_going_on_bad_input():
    applied = []
    current = {"value": config(rule("r1"))}

    def fetch():
        return current["value"]

    def apply(cfg):
        if cfg.get("rules") == "broken":
            raise ValueError("bad rules")
        applied.append(cfg)

    watcher = RecipeWatcher(fetch=fetch, apply=apply, interval_seconds=0, initial=config(rule("r1")))
    assert watcher.check_once() == "unchanged" and applied == []
    current["value"] = config(rule("r1", enabled=False))
    assert watcher.check_once() == "reloaded" and len(applied) == 1 and watcher.reloads == 1
    assert watcher.check_once() == "unchanged"
    current["value"] = config("broken")
    current["value"]["rules"] = "broken"
    assert watcher.check_once() == "invalid" and len(applied) == 1  # last good config stays
    current["value"] = None
    assert watcher.check_once() == "unavailable"

    def failing_fetch():
        raise RuntimeError("gms down")

    assert RecipeWatcher(fetch=failing_fetch, apply=apply).check_once() == "unavailable"


def test_action_apply_config_swaps_rules_and_keeps_idempotency_state(fake_graph):
    class G:
        graph = fake_graph

    class C:
        graph = G()

    action = WorkflowActionsAction.create({**config(rule("r1")), "inMemoryState": True}, C())
    state = action.engine.state
    event = {
        "entityType": "actionRequest",
        "entityUrn": "urn:li:actionRequest:req-1",
        "category": "LIFECYCLE",
        "operation": "COMPLETED",
        "auditStamp": {"time": 1},
        "parameters": {"workflowUrn": WF, "result": "ACCEPTED", "actionRequestType": "WORKFLOW_FORM_REQUEST", "entityUrn": DATASET},
    }
    runs = action.handle_event(event)
    assert [r.ruleId for r in runs if r.fired] == ["r1"]
    tags_before = len(fake_graph.calls)

    # a save in the builder: r1 disabled, r2 added
    action.apply_config(config(rule("r1", enabled=False), rule("r2", tag="urn:li:tag:b")))
    assert action.engine.state is state  # same idempotency store
    assert [r.id for r in action.config.rules] == ["r1", "r2"]
    runs = action.handle_event(event)
    by_id = {r.ruleId: r for r in runs}
    assert by_id["r1"].fired is False and by_id["r1"].reason == "disabled"
    assert by_id["r2"].fired is True and len(fake_graph.calls) == tags_before + 1

    # the same event again for r2 is deduplicated by the retained state
    action.handle_event(event)
    assert len(fake_graph.calls) == tags_before + 1

    # an invalid recipe raises (the watcher turns that into "invalid") and changes nothing
    try:
        action.apply_config({"schemaVersion": 1, "rules": [{"id": "x"}]})
        assert False, "invalid rules must raise"
    except Exception:
        pass
    assert [r.id for r in action.config.rules] == ["r1", "r2"]
