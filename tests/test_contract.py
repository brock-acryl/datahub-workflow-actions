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
