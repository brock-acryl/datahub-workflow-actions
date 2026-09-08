import json

from datahub_workflow_actions.context import (
    StaticResolver,
    build_context,
    build_event_context,
    change_doc,
    event_id,
    event_view,
    is_workflow_lifecycle_event,
    parse_parameters,
)
from tests.conftest import ADMIN, DATASET, REQ, TAG_PII, WF, completed_event, deprecation_event, field_tag_added_event, tag_added_event


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


def test_graph_resolver_uses_the_action_request_root_query():
    """GMS exposes `actionRequest(urn)` — ActionRequest is not an Entity, so an
    `entity { ... on ActionRequest }` spread is rejected by the schema."""
    from datahub_workflow_actions.context import ACTION_REQUEST_QUERY, GraphResolver

    class Graph:
        def __init__(self):
            self.calls = []

        def execute_graphql(self, query, variables=None, **_):
            self.calls.append((query, variables))
            if "actionRequest(urn: $urn)" in query:
                return {"actionRequest": {"urn": variables["urn"], "status": "COMPLETED", "params": {"workflowFormRequest": {"workflowUrn": "urn:li:actionWorkflow:w"}}}}
            return {}

    assert "... on ActionRequest" not in ACTION_REQUEST_QUERY
    graph = Graph()
    resolver = GraphResolver(graph)
    request = resolver.get_action_request("urn:li:actionRequest:r1")
    assert request["status"] == "COMPLETED" and request["params"]["workflowFormRequest"]["workflowUrn"] == "urn:li:actionWorkflow:w"
    resolver.get_action_request("urn:li:actionRequest:r1")
    assert len(graph.calls) == 1  # cached per instance


def test_entity_query_spreads_the_owner_union():
    """`owner` is the OwnerType union (CorpUser | CorpGroup); a bare `owner { urn }` is rejected by GMS."""
    from datahub_workflow_actions.context import ENTITY_QUERY

    assert "owner { urn }" not in ENTITY_QUERY
    assert "owner { ... on CorpUser { urn } ... on CorpGroup { urn } }" in ENTITY_QUERY


# ---------------------------------------------------------------------------
# §21 event-shaped context
# ---------------------------------------------------------------------------


def test_event_context_shape(fixtures):
    ctx = build_context(tag_added_event(), StaticResolver(fixtures))
    assert set(ctx) == {"event", "entity", "actor", "change", "params"}  # no workflow/request/requester/form keys
    ev = ctx["event"]
    assert ev["type"] == "EntityChangeEvent" and ev["category"] == "TAG" and ev["operation"] == "ADD"
    assert ev["modifier"] == TAG_PII and ev["entityType"] == "dataset" and ev["entityUrn"] == DATASET
    assert ev["actor"] == ADMIN and ev["time"] == 1754000000000 and ev["parameters"]["tagUrn"] == TAG_PII
    assert ev["id"] == f"{DATASET}:TAG:ADD:{TAG_PII}:1754000000000"
    assert ctx["entity"]["name"] == "Orders" and ctx["entity"]["platform"] == "Snowflake" and ctx["entity"]["parent"] is None
    assert ctx["actor"]["username"] == "admin" and ctx["actor"]["email"] == "admin@example.com"
    assert ctx["change"]["tag"] == TAG_PII and ctx["change"]["term"] is None
    assert ctx["change"]["subject"] == {"urn": DATASET, "type": "dataset", "name": "db.sales.orders"}


def test_event_ids_are_unique_per_change_and_workflow_ids_are_unchanged():
    same_ms = 1754000000000
    a = event_id(tag_added_event(tag="urn:li:tag:a", time=same_ms))
    b = event_id(tag_added_event(tag="urn:li:tag:b", time=same_ms))
    c = event_id({**tag_added_event(tag="urn:li:tag:a", time=same_ms), "category": "GLOSSARY_TERM"})
    assert len({a, b, c}) == 3
    assert event_id(completed_event()) == f"{REQ}:COMPLETED:1754000000000"  # existing SQLite rows stay valid
    assert build_context(completed_event(), StaticResolver({}))["event"]["id"] == event_id(completed_event())


def test_field_tag_event_exposes_parent_and_field_path():
    ctx = build_event_context(field_tag_added_event(), StaticResolver({}))
    assert ctx["entity"]["parent"] == {"urn": DATASET} and ctx["entity"]["fieldPath"] == "customer_email"
    assert ctx["entity"]["type"] == "schemafield"
    assert ctx["change"]["parent"] == DATASET and ctx["change"]["field"] == "customer_email"


def test_parameters_are_json_decoded_and_view_is_lookup_free():
    raw = {"propertyUrn": "urn:li:structuredProperty:tier", "propertyValues": json.dumps(["gold"]), "note": "{not json", "owners": json.dumps([{"owner": "urn:li:corpuser:x"}])}
    params = parse_parameters(raw)
    assert params["propertyValues"] == ["gold"] and params["note"] == "{not json" and params["owners"][0]["owner"] == "urn:li:corpuser:x"
    view = event_view({**tag_added_event(), "parameters": raw})
    assert view["parameters"]["propertyValues"] == ["gold"] and view["actor"] == ADMIN and view["category"] == "TAG"


def test_change_vocabulary_covers_every_category():
    keys = set(change_doc(tag_added_event(), {}))
    for category, params, expect in [
        ("GLOSSARY_TERM", {"termUrn": "urn:li:glossaryTerm:t"}, ("term", "urn:li:glossaryTerm:t")),
        ("OWNERSHIP", {"ownerUrn": "urn:li:corpuser:o", "ownerType": "urn:li:ownershipType:__system__technical_owner"}, ("owner", "urn:li:corpuser:o")),
        ("DOMAIN", {"domainUrn": "urn:li:domain:d"}, ("domain", "urn:li:domain:d")),
        ("DEPRECATION", {"status": "DEPRECATED", "note": "bye"}, ("status", "DEPRECATED")),
        ("STRUCTURED_PROPERTY", {"propertyUrn": "urn:li:structuredProperty:p", "propertyValues": ["a"]}, ("values", ["a"])),
        ("TECHNICAL_SCHEMA", {"fieldPath": "id", "modificationCategory": "RENAME"}, ("modificationCategory", "RENAME")),
        ("DOCUMENTATION", {"description": "new", "previousDescription": "old"}, ("previousDescription", "old")),
        ("RUN", {"assertionResult": "FAILURE", "asserteeUrn": DATASET}, ("assertee", DATASET)),
        ("INCIDENT", {"type": "FRESHNESS", "title": "Late", "stage": "TRIAGE"}, ("incident", {"type": "FRESHNESS", "title": "Late", "stage": "TRIAGE", "entities": None})),
    ]:
        doc = change_doc({"category": category, "entityUrn": DATASET, "entityType": "dataset"}, params)
        assert set(doc) == keys, category  # every key always present
        assert doc[expect[0]] == expect[1], category
    # the modifier fills in when GMS omits the parameter
    assert change_doc({"category": "TAG", "modifier": TAG_PII}, {})["tag"] == TAG_PII
    assert change_doc(deprecation_event(), deprecation_event()["parameters"])["note"] == "Use orders_v2"


def test_proposals_and_other_action_requests_go_through_the_event_path():
    proposal = {**completed_event(), "parameters": {**completed_event()["parameters"], "actionRequestType": "TAG_ASSOCIATION"}}
    assert not is_workflow_lifecycle_event(proposal)
    ctx = build_context(proposal, StaticResolver({}))
    assert "workflow" not in ctx and ctx["event"]["category"] == "LIFECYCLE" and ctx["event"]["entityType"] == "actionRequest"
