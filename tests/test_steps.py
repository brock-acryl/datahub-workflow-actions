import json

import responses

from datahub_workflow_actions.steps import RunContext, catalog, get_step, known_step_types
from tests.conftest import DATASET


def run(type_, params, ctx):
    definition = get_step(type_)
    return definition.run(definition.params.model_validate(params), ctx)


def test_split_list_keeps_dataset_urns_whole():
    from datahub_workflow_actions.steps.metadata import split_list

    assert split_list(f"{DATASET}, urn:li:tag:a ,urn:li:dataset:(urn:li:dataPlatform:hive,db.t,PROD)") == [
        DATASET, "urn:li:tag:a", "urn:li:dataset:(urn:li:dataPlatform:hive,db.t,PROD)"
    ]
    assert split_list("") == []


def test_catalog_lists_every_step_with_params_and_outputs():
    types = known_step_types()
    for expected in ["add_tag", "remove_tag", "add_term", "remove_term", "add_owner", "remove_owner", "set_domain", "clear_domain", "add_to_data_product", "set_structured_property", "deprecate", "undeprecate", "update_description", "webhook", "slack", "teams", "email", "jira_issue", "wait"]:
        assert expected in types
    entry = next(c for c in catalog() if c["type"] == "webhook")
    assert "url" in entry["params"] and entry["outputs"]["status"]


def test_metadata_steps_call_the_right_mutations(fake_graph):
    ctx = RunContext(graph=fake_graph)
    run("add_tag", {"entity": DATASET, "tag": "urn:li:tag:a, urn:li:tag:b"}, ctx)
    run("add_owner", {"entity": DATASET, "owner": "urn:li:corpGroup:g", "ownershipType": "BUSINESS_OWNER"}, ctx)
    run("add_owner", {"entity": DATASET, "owner": "urn:li:corpuser:u", "ownershipType": "urn:li:ownershipType:custom"}, ctx)
    run("set_domain", {"entity": DATASET, "domain": "urn:li:domain:d"}, ctx)
    run("add_to_data_product", {"entity": [DATASET], "dataProduct": "urn:li:dataProduct:p"}, ctx)
    run("set_structured_property", {"entity": DATASET, "property": "urn:li:structuredProperty:sp", "values": "3, high"}, ctx)
    run("deprecate", {"entity": DATASET, "note": "bye", "replacement": "urn:li:dataset:new"}, ctx)
    run("update_description", {"entity": DATASET, "description": "d"}, ctx)
    run("remove_owner", {"entity": DATASET, "owner": "urn:li:corpuser:u"}, ctx)
    run("clear_domain", {"entity": DATASET}, ctx)
    mutations = [q.split("{", 1)[1].split("(", 1)[0].strip() for q, _ in fake_graph.calls]
    assert mutations == ["batchAddTags", "batchAddOwners", "batchAddOwners", "batchSetDomain", "batchSetDataProduct", "upsertStructuredProperties", "updateDeprecation", "updateDescription", "batchRemoveOwners", "unsetDomain"]
    tag_vars = fake_graph.calls[0][1]["input"]
    assert tag_vars == {"tagUrns": ["urn:li:tag:a", "urn:li:tag:b"], "resources": [{"resourceUrn": DATASET}]}
    owners_vars = fake_graph.calls[1][1]["input"]["owners"][0]
    assert owners_vars == {"ownerUrn": "urn:li:corpGroup:g", "ownerEntityType": "CORP_GROUP", "type": "BUSINESS_OWNER"}
    custom_owner = fake_graph.calls[2][1]["input"]["owners"][0]
    assert custom_owner["ownershipTypeUrn"] == "urn:li:ownershipType:custom" and "type" not in custom_owner
    sp_values = fake_graph.calls[5][1]["input"]["structuredPropertyInputParams"][0]["values"]
    assert sp_values == [{"numberValue": 3.0}, {"stringValue": "high"}]
    dep = fake_graph.calls[6][1]["input"]
    assert dep == {"urn": DATASET, "deprecated": True, "note": "bye", "replacement": "urn:li:dataset:new"}


def test_metadata_steps_dry_run_without_graph():
    out = run("add_term", {"entity": DATASET, "term": "urn:li:glossaryTerm:t"}, RunContext(dry_run=True))
    assert out["dryRun"] and out["mutation"] == "batchAddTerms"


@responses.activate
def test_webhook_posts_json_and_returns_outputs():
    responses.add(responses.POST, "https://hooks.example/x", json={"key": "DATA-1"}, status=201, headers={"X-Id": "7"})
    out = run("webhook", {"url": "https://hooks.example/x", "headers": '{"Authorization": "Bearer t"}', "body": {"entity": DATASET}}, RunContext())
    assert out["status"] == 201 and out["body"] == {"key": "DATA-1"} and out["headers"]["X-Id"] == "7"
    sent = responses.calls[0].request
    assert sent.headers["Authorization"] == "Bearer t" and json.loads(sent.body) == {"entity": DATASET}


@responses.activate
def test_webhook_unexpected_status_raises():
    responses.add(responses.POST, "https://hooks.example/x", body="nope", status=500)
    try:
        run("webhook", {"url": "https://hooks.example/x", "body": "{}"}, RunContext())
    except RuntimeError as e:
        assert "500" in str(e)
    else:
        raise AssertionError("expected failure")


@responses.activate
def test_slack_bot_token_and_webhook_modes():
    responses.add(responses.POST, "https://slack.com/api/chat.postMessage", json={"ok": True, "ts": "1.2", "channel": "C1"})
    out = run("slack", {"channel": "#data", "text": "hi"}, RunContext(env={"SLACK_BOT_TOKEN": "xoxb"}))
    assert out == {"ts": "1.2", "channel": "C1"}
    assert responses.calls[0].request.headers["Authorization"] == "Bearer xoxb"
    responses.add(responses.POST, "https://hooks.slack.com/services/abc", body="ok")
    assert run("slack", {"webhookUrl": "https://hooks.slack.com/services/abc", "text": "hi"}, RunContext()) == {"ok": True}


def test_slack_requires_token_outside_dry_run():
    try:
        run("slack", {"channel": "#data", "text": "hi"}, RunContext(env={}))
    except ValueError as e:
        assert "SLACK_BOT_TOKEN" in str(e)
    else:
        raise AssertionError("expected failure")
    assert run("slack", {"channel": "#data", "text": "hi"}, RunContext(env={}, dry_run=True))["dryRun"]


@responses.activate
def test_jira_issue_returns_key_and_url():
    responses.add(responses.POST, "https://acme.atlassian.net/rest/api/2/issue", json={"key": "DATA-9", "id": "10001"}, status=201)
    out = run("jira_issue", {"baseUrl": "https://acme.atlassian.net/", "project": "DATA", "summary": "Grant", "description": "d"}, RunContext(env={"JIRA_EMAIL": "e", "JIRA_API_TOKEN": "t"}))
    assert out == {"key": "DATA-9", "id": "10001", "url": "https://acme.atlassian.net/browse/DATA-9"}


def test_wait_uses_injected_sleep():
    slept = []
    assert run("wait", {"seconds": 2}, RunContext(sleep=slept.append)) == {"waited": 2}
    assert slept == [2]
    assert run("wait", {"seconds": 2}, RunContext(sleep=slept.append, dry_run=True)) == {"waited": 2} and slept == [2]


def test_email_dry_run_lists_recipients():
    out = run("email", {"to": "a@x.com, b@x.com", "subject": "s", "body": "b"}, RunContext(env={"SMTP_FROM": "noreply@x.com"}, dry_run=True))
    assert out["to"] == ["a@x.com", "b@x.com"] and out["from"] == "noreply@x.com"
