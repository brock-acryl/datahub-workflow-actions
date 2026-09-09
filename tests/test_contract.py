import pytest
from pydantic import ValidationError

from datahub_workflow_actions.contract import RulesConfig, load_rules, rules_json_schema

RULE = {
    "id": "r1",
    "workflowUrn": "urn:li:actionWorkflow:wf-1",
    "on": {"operation": "COMPLETED", "result": "ACCEPTED"},
    "steps": [{"id": "tag", "type": "add_tag", "params": {"entity": "{{ entity.urn }}", "tag": "urn:li:tag:x"}}],
}


def test_load_rules_from_recipe_root_bare_config_and_list():
    recipe = {"pipeline_name": "x", "source": {"type": "datahub-workflow-actions", "config": {"schemaVersion": 1, "rules": [RULE], "extra_pip_requirements": ["a"]}}}
    assert [r.id for r in load_rules(recipe).rules] == ["r1"]
    assert load_rules({"rules": [RULE]}).rules[0].steps[0].type == "add_tag"
    assert load_rules([RULE]).rules[0].onError == "fail"
    assert load_rules('{"rules": []}').rules == []


def test_rejects_unknown_fields_bad_triggers_and_duplicates():
    with pytest.raises(ValidationError):
        RulesConfig.model_validate({"rules": [{**RULE, "bogus": 1}]})
    with pytest.raises(ValidationError, match="COMPLETED trigger needs a result"):
        RulesConfig.model_validate({"rules": [{**RULE, "on": {"operation": "COMPLETED"}}]})
    with pytest.raises(ValidationError, match="stepId only applies"):
        RulesConfig.model_validate({"rules": [{**RULE, "on": {"operation": "COMPLETED", "result": "ACCEPTED", "stepId": "s"}}]})
    with pytest.raises(ValidationError, match="duplicate rule id"):
        RulesConfig.model_validate({"rules": [RULE, RULE]})
    with pytest.raises(ValidationError, match="duplicate step id"):
        RulesConfig.model_validate({"rules": [{**RULE, "steps": [RULE["steps"][0], RULE["steps"][0]]}]})
    with pytest.raises(ValidationError, match="unsupported schemaVersion"):
        RulesConfig.model_validate({"schemaVersion": 9, "rules": []})


def test_nested_filter_groups_and_extended_step_options_parse():
    cfg = RulesConfig.model_validate(
        {
            "rules": [
                {
                    **RULE,
                    "when": {
                        "operator": "OR",
                        "filters": [
                            {"operator": "AND", "filters": [{"field": "a", "values": ["1"]}, {"field": "b", "condition": "EXISTS"}]},
                            {"field": "c", "condition": "IN", "values": ["x", "y"], "negated": True, "caseInsensitive": True},
                        ],
                    },
                    "steps": [
                        {
                            "id": "hook",
                            "type": "webhook",
                            "params": {"url": "https://x"},
                            "retry": {"attempts": 3, "backoff": "fixed", "delaySeconds": 0.5},
                            "timeoutSeconds": 5,
                            "forEach": "{{ entity.owners }}",
                            "idempotencyKey": "{{ event.id }}-{{ item }}",
                            "onError": "continue",
                            "description": "notify",
                        }
                    ],
                }
            ]
        }
    )
    step = cfg.rules[0].steps[0]
    assert step.retry.attempts == 3 and step.timeoutSeconds == 5 and step.forEach and step.onError == "continue"
    assert cfg.rules[0].when.filters[0].operator == "AND"


def test_filter_needs_values_unless_exists():
    with pytest.raises(ValidationError, match="needs at least one value"):
        RulesConfig.model_validate({"rules": [{**RULE, "when": {"operator": "AND", "filters": [{"field": "a"}]}}]})


def test_json_schema_is_published_with_id_and_covers_rules():
    schema = rules_json_schema()
    assert schema["$id"].endswith("/v1.json")
    assert "Rule" in schema["$defs"] and "RetryPolicy" in schema["$defs"] and "FilterGroup" in schema["$defs"]
    assert "forEach" in schema["$defs"]["Step"]["properties"]


def test_published_schema_and_catalog_carry_no_pydantic_titles():
    """Auto titles differ across pydantic versions; the contract must not depend on them."""
    import json

    from datahub_workflow_actions.contract import rules_json_schema
    from datahub_workflow_actions.steps import catalog

    def titles(node, path=""):
        if isinstance(node, dict):
            for k, v in node.items():
                if k == "title" and path != "":
                    yield path
                yield from titles(v, f"{path}/{k}")
        elif isinstance(node, list):
            for i, v in enumerate(node):
                yield from titles(v, f"{path}[{i}]")

    assert list(titles(rules_json_schema())) == []  # only the root title survives (path == "")
    assert rules_json_schema()["title"] == "DataHub workflow-actions rules"
    assert list(titles(json.loads(json.dumps(catalog())))) == []


def test_published_schema_normalises_version_dependent_details():
    from datahub_workflow_actions.contract import strip_auto_titles

    raw = {
        "title": "X",
        "properties": {
            "delay": {"type": "number", "minimum": 0.0, "maximum": 3600.0, "title": "Delay", "default": 1.0},
            "params": {"type": "object", "additionalProperties": True, "title": "Params"},
            "items": {"type": "array", "items": {"additionalProperties": False, "type": "object"}},
        },
    }
    assert strip_auto_titles(raw) == {
        "properties": {
            "delay": {"type": "number", "minimum": 0, "maximum": 3600, "default": 1.0},
            "params": {"type": "object"},
            "items": {"type": "array", "items": {"additionalProperties": False, "type": "object"}},
        }
    }


# ---- §21 event triggers -----------------------------------------------------------


def test_legacy_triggers_parse_as_workflow_triggers_and_round_trip():
    from datahub_workflow_actions.contract import Rule, WorkflowTrigger

    rule = Rule.model_validate(RULE)
    assert isinstance(rule.on, WorkflowTrigger) and rule.on.type == "workflow" and rule.trigger_type == "workflow"
    dumped = rule.model_dump(exclude_none=True)
    assert dumped["on"]["type"] == "workflow" and dumped["workflowUrn"] == RULE["workflowUrn"]
    assert Rule.model_validate(dumped).model_dump() == rule.model_dump()  # stable under re-parse


def test_event_trigger_parses_normalises_and_rejects_a_workflow_urn():
    from datahub_workflow_actions.contract import EventTrigger, Rule

    raw = {
        "id": "pii",
        "on": {
            "type": "event",
            "category": "tag",
            "operations": ["add"],
            "entityTypes": ["dataset", "schemaField"],
            "modifier": {"values": ["urn:li:tag:pii"]},
            "parameters": [{"field": "context", "condition": "EXISTS"}],
        },
        "steps": [],
    }
    rule = Rule.model_validate(raw)
    assert isinstance(rule.on, EventTrigger)
    assert rule.on.category == "TAG" and rule.on.operations == ["ADD"] and rule.on.entityTypes == ["dataset", "schemaField"]
    assert rule.on.modifier.condition == "EQUAL" and rule.on.ignoreOwnChanges is True and rule.workflowUrn is None
    with pytest.raises(ValueError, match="have no workflowUrn"):
        Rule.model_validate({**raw, "workflowUrn": "urn:li:actionWorkflow:w"})
    with pytest.raises(ValueError, match="needs workflowUrn"):
        Rule.model_validate({"id": "w", "on": {"operation": "CREATE"}, "steps": []})
    with pytest.raises(ValueError):
        Rule.model_validate({**raw, "on": {**raw["on"], "type": "webhook"}})
    with pytest.raises(ValueError, match="modifier match needs"):
        Rule.model_validate({**raw, "on": {**raw["on"], "modifier": {"values": []}}})
    with pytest.raises(ValueError):
        Rule.model_validate({**raw, "on": {**raw["on"], "category": ""}})


def test_rules_config_splits_workflow_and_event_rules():
    from datahub_workflow_actions.contract import RulesConfig

    cfg = RulesConfig.model_validate(
        {"schemaVersion": 1, "rules": [RULE, {"id": "e", "on": {"type": "event", "category": "OWNERSHIP"}, "steps": []}]}
    )
    assert [r.id for r in cfg.workflow_rules()] == [RULE["id"]]
    assert [r.id for r in cfg.event_rules()] == ["e"]
    assert cfg.rules_for(RULE["workflowUrn"]) == cfg.workflow_rules()


def test_schema_publishes_the_trigger_union_and_optional_workflow_urn():
    from datahub_workflow_actions.contract import KNOWN_TRIGGERS, rules_json_schema, triggers_catalog

    schema = rules_json_schema()
    rule = schema["$defs"]["Rule"]
    assert "workflowUrn" not in rule["required"] and "on" in rule["required"]
    on = rule["properties"]["on"]
    assert on["discriminator"]["propertyName"] == "type"
    assert set(on["discriminator"]["mapping"]) == {"workflow", "event", "schedule"}
    assert {"WorkflowTrigger", "EventTrigger", "ValueMatch"} <= set(schema["$defs"])
    assert schema["$defs"]["WorkflowTrigger"]["properties"]["type"]["default"] == "workflow"
    assert "type" not in schema["$defs"]["WorkflowTrigger"].get("required", [])  # legacy recipes omit it
    assert "type" in schema["$defs"]["EventTrigger"]["required"]
    catalog = triggers_catalog()
    assert set(catalog["categories"]) == set(KNOWN_TRIGGERS) and "dataset" in catalog["entityTypes"]
    assert catalog["categories"]["OWNERSHIP"]["operations"] == ["ADD", "REMOVE"]


# ---------------------------------------------------------------------------
# §21 E4 schedule trigger
# ---------------------------------------------------------------------------


def test_schedule_trigger_parses_and_validates():
    cfg = load_rules({"schemaVersion": 1, "rules": [{
        "id": "nightly", "on": {"type": "schedule", "cron": "0 6 * * 1-5", "timezone": "Europe/Berlin"},
        "steps": [{"id": "s", "type": "search", "params": {"query": "*"}}]}]})
    rule = cfg.rules[0]
    assert rule.trigger_type == "schedule" and rule.on.cron == "0 6 * * 1-5" and rule.on.timezone == "Europe/Berlin" and rule.on.catchUp is False
    assert cfg.schedule_rules() == [rule] and cfg.event_rules() == [] and cfg.workflow_rules() == []
    with pytest.raises(ValidationError, match="invalid cron"):
        load_rules({"schemaVersion": 1, "rules": [{"id": "x", "on": {"type": "schedule", "cron": "every day"}}]})
    with pytest.raises(ValidationError, match="unknown timezone"):
        load_rules({"schemaVersion": 1, "rules": [{"id": "x", "on": {"type": "schedule", "cron": "* * * * *", "timezone": "Mars/Olympus"}}]})
    with pytest.raises(ValidationError, match="have no workflowUrn"):
        load_rules({"schemaVersion": 1, "rules": [{"id": "x", "workflowUrn": "urn:li:actionWorkflow:w", "on": {"type": "schedule", "cron": "* * * * *"}}]})
    schema = rules_json_schema()
    assert "ScheduleTrigger" in schema["$defs"] and schema["$defs"]["ScheduleTrigger"]["properties"]["cron"]


# ---------------------------------------------------------------------------
# §22 branches
# ---------------------------------------------------------------------------

BRANCH = {
    "id": "b", "type": "branch",
    "if": {"operator": "AND", "filters": [{"field": "entity.platform", "values": ["Snowflake"]}]},
    "then": [{"id": "yes", "type": "add_tag", "params": {"tag": "urn:li:tag:snow"}}],
    "else": [{"id": "no", "type": "add_tag", "params": {"tag": "urn:li:tag:other"}}],
}


def branch_rule(*steps):
    return load_rules({"schemaVersion": 1, "rules": [{"id": "r", "workflowUrn": "urn:li:actionWorkflow:w", "on": {"operation": "CREATE"}, "steps": list(steps)}]}).rules[0]


def test_branch_parses_by_alias_and_by_attribute_and_round_trips():
    rule = branch_rule(BRANCH, {"id": "after", "type": "wait", "params": {"seconds": 1}})
    b = rule.steps[0]
    assert b.is_branch and b.if_.filters[0].field == "entity.platform"
    assert [s.id for s in b.then_steps] == ["yes"] and [s.id for s in b.else_steps] == ["no"]
    assert [s.id for s in rule.all_steps()] == ["b", "yes", "no", "after"]
    # python attribute names work too, and dumps use the recipe spelling
    by_attr = load_rules({"schemaVersion": 1, "rules": [{"id": "r", "workflowUrn": "urn:li:actionWorkflow:w", "on": {"operation": "CREATE"},
        "steps": [{"id": "b", "type": "branch", "if_": BRANCH["if"], "then": [], "else_": []}]}]}).rules[0]
    dumped = by_attr.steps[0].model_dump(exclude_none=True, by_alias=True)
    assert "if" in dumped and "if_" not in dumped and dumped["then"] == [] and dumped["else"] == []
    # a plain step dumps without branch keys at all
    assert "then" not in rule.steps[1].model_dump(exclude_none=True, by_alias=True)


def test_nested_branches_and_depth_limit():
    def nest(depth):
        step = {"id": f"leaf{depth}", "type": "wait", "params": {"seconds": 1}}
        for level in range(depth, 0, -1):
            step = {"id": f"b{level}", "type": "branch", "if": BRANCH["if"], "then": [step], "else": []}
        return step
    rule = branch_rule(nest(5))
    assert [d for _, d, _ in __import__("datahub_workflow_actions.contract", fromlist=["iter_steps"]).iter_steps(rule.steps)] == [0, 1, 2, 3, 4, 5]
    with pytest.raises(ValidationError, match="nested deeper than 5"):
        branch_rule(nest(6))
    # the lane path is reported
    from datahub_workflow_actions.contract import iter_steps
    paths = {s.id: p for s, _, p in iter_steps(branch_rule(BRANCH).steps)}
    assert paths["no"] == ("b", "else") and paths["yes"] == ("b", "then") and paths["b"] == ()


def test_branch_constraints():
    with pytest.raises(ValidationError, match="a branch has no params"):
        branch_rule({**BRANCH, "params": {"x": 1}})
    with pytest.raises(ValidationError, match="`forEach` does not apply"):
        branch_rule({**BRANCH, "forEach": "{{ entity.owners }}"})
    with pytest.raises(ValidationError, match="`retry` does not apply"):
        branch_rule({**BRANCH, "retry": {"attempts": 2}})
    with pytest.raises(ValidationError, match="at least one condition"):
        branch_rule({**BRANCH, "if": {"operator": "AND", "filters": []}})
    with pytest.raises(ValidationError, match="at least one condition"):
        branch_rule({k: v for k, v in BRANCH.items() if k != "if"})
    with pytest.raises(ValidationError, match="only apply to `type: branch`"):
        branch_rule({"id": "x", "type": "wait", "params": {"seconds": 1}, "then": []})
    with pytest.raises(ValidationError, match="duplicate step id 'no'"):
        branch_rule(BRANCH, {"id": "no", "type": "wait", "params": {"seconds": 1}})
    # lanes may be empty (the builder warns, the engine just continues)
    assert branch_rule({**BRANCH, "then": [], "else": []}).steps[0].then_steps == []


def test_schema_publishes_a_recursive_step():
    step = rules_json_schema()["$defs"]["Step"]
    assert set(step["properties"]) >= {"if", "then", "else"}
    assert step["properties"]["then"]["anyOf"][0]["items"] == {"$ref": "#/$defs/Step"}
    assert "if_" not in step["properties"] and "else_" not in step["properties"]
