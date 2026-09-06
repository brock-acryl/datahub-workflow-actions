import json
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]

WF = "urn:li:actionWorkflow:wf-1"
REQ = "urn:li:actionRequest:req-1"
DATASET = "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.sales.orders,PROD)"


def completed_event(result="ACCEPTED", **extra_params):
    return {
        "auditStamp": {"actor": "urn:li:corpuser:approver", "time": 1754000000000},
        "entityUrn": REQ,
        "entityType": "actionRequest",
        "category": "LIFECYCLE",
        "operation": "COMPLETED",
        "version": 0,
        "parameters": {
            "actorUrn": "urn:li:corpuser:approver",
            "actionRequestType": "WORKFLOW_FORM_REQUEST",
            "workflowUrn": WF,
            "workflowId": "wf-1",
            "entityUrn": DATASET,
            "entityType": "dataset",
            "fields": json.dumps({"field_abc": ["restricted"], "field_multi": ["x", "y"]}),
            "result": result,
            "operation": "COMPLETE",
            **extra_params,
        },
    }


FIXTURES = {
    REQ: {
        "urn": REQ,
        "description": "Please grant access",
        "status": "COMPLETED",
        "result": "ACCEPTED",
        "entity": {"urn": DATASET, "type": "DATASET"},
        "created": {"time": 1753000000000, "actor": {"urn": "urn:li:corpuser:jdoe", "username": "jdoe"}},
        "params": {
            "workflowFormRequest": {
                "workflowUrn": WF,
                "workflow": {
                    "urn": WF,
                    "name": "Access Request",
                    "steps": [{"id": "step-1", "description": "Manager approval"}],
                    "trigger": {"form": {"fields": [{"id": "field_abc", "name": "Tier"}, {"id": "field_multi", "name": "Systems"}]}},
                },
                "fields": [],
                "decisions": [{"stepId": "step-1", "result": "ACCEPTED", "note": "ok", "timestamp": 1, "decidedBy": {"urn": "urn:li:corpuser:approver"}}],
            }
        },
    },
    DATASET: {
        "urn": DATASET,
        "type": "DATASET",
        "name": "db.sales.orders",
        "properties": {"name": "Orders", "description": "Sales orders"},
        "platform": {"name": "snowflake", "properties": {"displayName": "Snowflake"}},
        "tags": {"tags": [{"tag": {"urn": "urn:li:tag:pii"}}]},
        "ownership": {"owners": [{"owner": {"urn": "urn:li:corpuser:owner1"}}, {"owner": {"urn": "urn:li:corpGroup:data-eng"}}]},
        "domain": {"domain": {"urn": "urn:li:domain:sales"}},
    },
    "urn:li:corpuser:jdoe": {
        "urn": "urn:li:corpuser:jdoe",
        "username": "jdoe",
        "properties": {"displayName": "Jane Doe", "email": "jdoe@example.com"},
        "relationships": {"relationships": [{"entity": {"urn": "urn:li:corpGroup:analysts"}}]},
    },
    "urn:li:corpuser:approver": {"urn": "urn:li:corpuser:approver", "username": "approver", "properties": {"displayName": "Al Approver"}},
}


@pytest.fixture
def fixtures():
    return json.loads(json.dumps(FIXTURES))


@pytest.fixture
def context(fixtures):
    from datahub_workflow_actions.context import StaticResolver, build_context

    return build_context(completed_event(), StaticResolver(fixtures))


class FakeGraph:
    def __init__(self, fail_times=0):
        self.calls = []
        self.fail_times = fail_times

    def execute_graphql(self, query, variables=None, **_):
        self.calls.append((query, variables))
        if self.fail_times > 0:
            self.fail_times -= 1
            raise RuntimeError("transient")
        name = query.split("{", 1)[1].split("(", 1)[0].strip()
        if query.lstrip().startswith("query"):
            return {name: None}
        return {name: True}


@pytest.fixture
def fake_graph():
    return FakeGraph()
