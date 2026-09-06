from datahub_workflow_actions.context import StaticResolver, build_context, is_workflow_lifecycle_event
from tests.conftest import DATASET, REQ, WF, completed_event


def test_context_document_shape(context):
    assert context["event"]["operation"] == "COMPLETED" and context["event"]["result"] == "ACCEPTED"
    assert context["event"]["id"] == f"{REQ}:COMPLETED:1754000000000"
    assert context["request"]["urn"] == REQ and context["request"]["description"] == "Please grant access"
    assert context["workflow"] == {
        "urn": WF, "id": "wf-1", "name": "Access Request",
        "steps": [{"id": "step-1", "description": "Manager approval"}],
        "fields": [{"id": "field_abc", "name": "Tier"}, {"id": "field_multi", "name": "Systems"}],
    }
    assert context["form"] == {"field_abc": "restricted", "field_multi": ["x", "y"]}
    assert context["form_by_name"] == {"Tier": "restricted", "Systems": ["x", "y"]}
    assert context["entity"]["urn"] == DATASET and context["entity"]["name"] == "Orders" and context["entity"]["platform"] == "Snowflake"
    assert context["entity"]["owners"] == ["urn:li:corpuser:owner1", "urn:li:corpGroup:data-eng"] and context["entity"]["domain"] == "urn:li:domain:sales"
    assert context["requester"]["email"] == "jdoe@example.com" and context["requester"]["groups"] == ["urn:li:corpGroup:analysts"]
    assert context["approver"]["name"] == "Al Approver"
    assert context["decisions"][0]["decidedBy"] == "urn:li:corpuser:approver"


def test_context_degrades_without_fixtures():
    ctx = build_context(completed_event(), StaticResolver({}))
    assert ctx["workflow"]["urn"] == WF and ctx["workflow"]["name"] is None
    assert ctx["entity"]["urn"] == DATASET and ctx["entity"]["type"] == "dataset"
    assert ctx["entity"]["name"] == "db.sales.orders"  # urn_name fallback, not the raw urn tail
    assert ctx["form"]["field_abc"] == "restricted"
    assert ctx["requester"] == {} and ctx["approver"]["username"] == "approver"


def test_step_and_cancelled_results():
    ev = completed_event()
    ev["operation"] = "MODIFY"
    ev["parameters"].pop("result")
    ev["parameters"].update({"stepId": "step-1", "stepResult": "REJECTED"})
    ctx = build_context(ev, StaticResolver({}))
    assert ctx["event"]["stepId"] == "step-1" and ctx["event"]["result"] == "REJECTED"
    cancelled = completed_event()
    cancelled["parameters"].pop("result")
    cancelled["parameters"]["actionRequestStatus"] = "CANCELLED"
    assert build_context(cancelled, StaticResolver({}))["event"]["result"] == "CANCELLED"


def test_event_filter():
    assert is_workflow_lifecycle_event(completed_event())
    other = completed_event()
    other["entityType"] = "dataset"
    assert not is_workflow_lifecycle_event(other)
    proposal = completed_event()
    proposal["parameters"]["actionRequestType"] = "TAG_ASSOCIATION"
    assert not is_workflow_lifecycle_event(proposal)
