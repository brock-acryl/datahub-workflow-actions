"""Turn an actionRequest EntityChangeEvent into the context document that
filters and templates read.

    event      operation, result, stepId, time, actor, id
    request    urn, id, description, status, result, resultNote, createdAt
    workflow   urn, id, name, steps[{id, description}], fields[{id, name}]
    entity     urn, type, name, platform, description, tags[], terms[], owners[], domain
    form       {fieldId: value | [values]}      form_by_name {name: value}
    requester  urn, username, email, name, groups[]
    approver   urn, username, email, name           (event actor on MODIFY/COMPLETED)
    decisions  [{stepId, result, note, decidedBy, timestamp}]
    params     the raw event parameters

Resolution goes through a ``Resolver`` so the engine can run against GMS
(``GraphResolver``) or fixtures (``StaticResolver``, used by ``simulate``).
"""

from __future__ import annotations

import json
import logging
from typing import Any, Dict, Mapping, Optional, Protocol

from datahub_workflow_actions.templating import urn_name

logger = logging.getLogger(__name__)

ACTION_REQUEST_ENTITY_TYPE = "actionRequest"
LIFECYCLE_CATEGORY = "LIFECYCLE"
WORKFLOW_FORM_REQUEST = "WORKFLOW_FORM_REQUEST"


class Resolver(Protocol):
    def get_action_request(self, urn: str) -> Optional[dict]: ...
    def get_workflow(self, urn: str) -> Optional[dict]: ...
    def get_entity(self, urn: str) -> Optional[dict]: ...
    def get_user(self, urn: str) -> Optional[dict]: ...


class StaticResolver:
    """Fixtures keyed by urn — for simulate and tests."""

    def __init__(self, fixtures: Optional[Mapping[str, dict]] = None):
        self.fixtures: Dict[str, dict] = dict(fixtures or {})

    def _get(self, urn: str) -> Optional[dict]:
        value = self.fixtures.get(urn)
        return value if isinstance(value, dict) else None

    get_action_request = get_workflow = get_entity = get_user = _get


ACTION_REQUEST_QUERY = """
query workflowActionsRequest($urn: String!) {
  entity(urn: $urn) {
    urn
    ... on ActionRequest {
      description status result resultNote
      entity { urn type }
      created { time actor { urn username } }
      params { workflowFormRequest {
        workflowUrn
        workflow { urn name steps { id description } trigger { form { fields { id name } } } }
        fields { id values { ... on StringValue { stringValue } ... on NumberValue { numberValue } } }
        decisions { stepId result note timestamp decidedBy { urn username } }
      } }
    }
  }
}"""

WORKFLOW_QUERY = """
query workflowActionsWorkflow($urn: String!) {
  entity(urn: $urn) {
    urn
    ... on ActionWorkflow { name steps { id description } trigger { form { fields { id name } } } }
  }
}"""

ENTITY_QUERY = """
query workflowActionsEntity($urn: String!) {
  entity(urn: $urn) {
    urn type
    ... on Dataset { name properties { name description } platform { name properties { displayName } }
      tags { tags { tag { urn } } } glossaryTerms { terms { term { urn } } }
      ownership { owners { owner { urn } ownershipType { urn } type } } domain { domain { urn } } }
    ... on Dashboard { properties { name description } platform { name } tags { tags { tag { urn } } }
      glossaryTerms { terms { term { urn } } } ownership { owners { owner { urn } } } domain { domain { urn } } }
    ... on Chart { properties { name description } platform { name } tags { tags { tag { urn } } }
      glossaryTerms { terms { term { urn } } } ownership { owners { owner { urn } } } domain { domain { urn } } }
    ... on Container { properties { name description } platform { name } tags { tags { tag { urn } } }
      glossaryTerms { terms { term { urn } } } ownership { owners { owner { urn } } } domain { domain { urn } } }
    ... on DataJob { properties { name description } tags { tags { tag { urn } } }
      ownership { owners { owner { urn } } } domain { domain { urn } } }
    ... on DataFlow { properties { name description } tags { tags { tag { urn } } }
      ownership { owners { owner { urn } } } domain { domain { urn } } }
    ... on GlossaryTerm { properties { name description } ownership { owners { owner { urn } } } }
    ... on GlossaryNode { properties { name description } ownership { owners { owner { urn } } } }
    ... on Domain { properties { name description } ownership { owners { owner { urn } } } }
    ... on DataProduct { properties { name description } ownership { owners { owner { urn } } } domain { domain { urn } } }
  }
}"""

USER_QUERY = """
query workflowActionsUser($urn: String!) {
  corpUser(urn: $urn) {
    urn username
    properties { displayName fullName email }
    info { displayName fullName email }
    editableProperties { displayName email }
    relationships(input: { types: ["IsMemberOfGroup", "IsMemberOfNativeGroup"], direction: OUTGOING, start: 0, count: 100 }) {
      relationships { entity { urn } }
    }
  }
}"""


class GraphResolver:
    """Resolves through ``DataHubGraph.execute_graphql`` with a per-instance cache."""

    def __init__(self, graph: Any):
        self.graph = graph
        self._cache: Dict[str, Any] = {}

    def _query(self, query: str, urn: str, root: str) -> Optional[dict]:
        key = f"{root}:{urn}"
        if key in self._cache:
            return self._cache[key]
        try:
            data = self.graph.execute_graphql(query, variables={"urn": urn}) or {}
            result = data.get(root) if isinstance(data, dict) else None
            if not isinstance(result, dict):
                result = None
        except Exception as e:  # noqa: BLE001 — resolution is best-effort
            logger.warning("workflow-actions: could not resolve %s %s: %s", root, urn, e)
            result = None
        self._cache[key] = result
        return result

    def get_action_request(self, urn: str) -> Optional[dict]:
        return self._query(ACTION_REQUEST_QUERY, urn, "entity")

    def get_workflow(self, urn: str) -> Optional[dict]:
        return self._query(WORKFLOW_QUERY, urn, "entity")

    def get_entity(self, urn: str) -> Optional[dict]:
        return self._query(ENTITY_QUERY, urn, "entity")

    def get_user(self, urn: str) -> Optional[dict]:
        return self._query(USER_QUERY, urn, "corpUser")


def is_workflow_lifecycle_event(event: Mapping[str, Any]) -> bool:
    params = event.get("parameters") or {}
    request_type = params.get("actionRequestType")
    return (
        event.get("entityType") == ACTION_REQUEST_ENTITY_TYPE
        and event.get("category") == LIFECYCLE_CATEGORY
        and (request_type in (None, WORKFLOW_FORM_REQUEST))
    )


def _parse_fields(raw: Any) -> Dict[str, Any]:
    """``parameters.fields`` is a JSON string keyed by field id; every value is a list."""
    if raw in (None, ""):
        return {}
    data = json.loads(raw) if isinstance(raw, str) else raw
    if not isinstance(data, dict):
        return {}
    form: Dict[str, Any] = {}
    for field_id, values in data.items():
        if isinstance(values, list):
            form[field_id] = values[0] if len(values) == 1 else values
        else:
            form[field_id] = values
    return form


def _fields_from_request(request: Optional[dict]) -> Dict[str, Any]:
    fields = ((request or {}).get("params") or {}).get("workflowFormRequest", {}).get("fields") or []
    form: Dict[str, Any] = {}
    for field in fields:
        values = []
        for value in field.get("values") or []:
            if "stringValue" in value and value["stringValue"] is not None:
                values.append(value["stringValue"])
            elif "numberValue" in value and value["numberValue"] is not None:
                values.append(value["numberValue"])
        form[field["id"]] = values[0] if len(values) == 1 else values
    return form


def _user_doc(urn: Optional[str], resolver: Resolver) -> Dict[str, Any]:
    if not urn:
        return {}
    raw = resolver.get_user(urn) or {}
    props = raw.get("properties") or {}
    info = raw.get("info") or {}
    editable = raw.get("editableProperties") or {}
    groups = [
        rel["entity"]["urn"]
        for rel in ((raw.get("relationships") or {}).get("relationships") or [])
        if rel.get("entity", {}).get("urn")
    ]
    username = raw.get("username") or urn.rsplit(":", 1)[-1]
    return {
        "urn": urn,
        "username": username,
        "email": editable.get("email") or props.get("email") or info.get("email"),
        "name": editable.get("displayName")
        or props.get("displayName")
        or props.get("fullName")
        or info.get("displayName")
        or info.get("fullName")
        or username,
        "groups": groups,
    }


def _entity_doc(urn: Optional[str], event_params: Mapping[str, Any], resolver: Resolver) -> Dict[str, Any]:
    if not urn:
        return {}
    raw = resolver.get_entity(urn) or {}
    props = raw.get("properties") or {}
    platform = raw.get("platform") or {}
    doc = {
        "urn": urn,
        "type": (raw.get("type") or event_params.get("entityType") or "").lower() or None,
        "name": props.get("name") or raw.get("name") or event_params.get("entityName") or urn_name(urn),
        "description": props.get("description"),
        "platform": (platform.get("properties") or {}).get("displayName") or platform.get("name"),
        "tags": [t["tag"]["urn"] for t in ((raw.get("tags") or {}).get("tags") or []) if t.get("tag")],
        "terms": [t["term"]["urn"] for t in ((raw.get("glossaryTerms") or {}).get("terms") or []) if t.get("term")],
        "owners": [o["owner"]["urn"] for o in ((raw.get("ownership") or {}).get("owners") or []) if o.get("owner")],
        "domain": ((raw.get("domain") or {}).get("domain") or {}).get("urn"),
    }
    return doc


def build_context(event: Mapping[str, Any], resolver: Resolver) -> Dict[str, Any]:
    """``event`` is the EntityChangeEvent as a plain dict (``EntityChangeEvent.to_obj()``)."""
    params: Dict[str, Any] = dict(event.get("parameters") or {})
    audit = event.get("auditStamp") or {}
    request_urn = event.get("entityUrn") or ""
    operation = event.get("operation")
    time = audit.get("time")
    actor_urn = params.get("actorUrn") or audit.get("actor")

    request_raw = resolver.get_action_request(request_urn) if request_urn else None
    form_request = ((request_raw or {}).get("params") or {}).get("workflowFormRequest") or {}
    workflow_urn = params.get("workflowUrn") or form_request.get("workflowUrn") or (form_request.get("workflow") or {}).get("urn")
    workflow_raw = form_request.get("workflow") or (resolver.get_workflow(workflow_urn) if workflow_urn else None) or {}

    result = params.get("result") or params.get("stepResult")
    status = params.get("actionRequestStatus") or (request_raw or {}).get("status")
    if operation == "COMPLETED" and not result and status == "CANCELLED":
        result = "CANCELLED"

    form = _parse_fields(params.get("fields")) or _fields_from_request(request_raw)
    field_names = {f["id"]: f.get("name") or f["id"] for f in ((workflow_raw.get("trigger") or {}).get("form") or {}).get("fields") or []}
    form_by_name = {field_names.get(field_id, field_id): value for field_id, value in form.items()}

    requester_urn = ((request_raw or {}).get("created") or {}).get("actor", {}).get("urn") or (
        actor_urn if operation == "CREATE" else None
    )
    entity_urn = params.get("entityUrn") or ((request_raw or {}).get("entity") or {}).get("urn")

    decisions = [
        {
            "stepId": d.get("stepId"),
            "result": d.get("result"),
            "note": d.get("note"),
            "timestamp": d.get("timestamp"),
            "decidedBy": (d.get("decidedBy") or {}).get("urn"),
        }
        for d in form_request.get("decisions") or []
    ]

    return {
        "event": {
            "type": "EntityChangeEvent",
            "id": f"{request_urn}:{operation}:{time}",
            "operation": operation,
            "result": result,
            "stepId": params.get("stepId"),
            "time": time,
            "actor": actor_urn,
            "category": event.get("category"),
        },
        "request": {
            "urn": request_urn,
            "id": request_urn.rsplit(":", 1)[-1] if request_urn else None,
            "description": (request_raw or {}).get("description"),
            "status": status,
            "result": (request_raw or {}).get("result") or result,
            "resultNote": (request_raw or {}).get("resultNote"),
            "createdAt": ((request_raw or {}).get("created") or {}).get("time"),
        },
        "workflow": {
            "urn": workflow_urn,
            "id": params.get("workflowId") or (workflow_urn.rsplit(":", 1)[-1] if workflow_urn else None),
            "name": workflow_raw.get("name"),
            "steps": [{"id": s.get("id"), "description": s.get("description")} for s in workflow_raw.get("steps") or []],
            "fields": [{"id": fid, "name": name} for fid, name in field_names.items()],
        },
        "entity": _entity_doc(entity_urn, params, resolver),
        "form": form,
        "form_by_name": form_by_name,
        "requester": _user_doc(requester_urn, resolver),
        "approver": _user_doc(actor_urn, resolver) if operation in ("MODIFY", "COMPLETED") else {},
        "decisions": decisions,
        "params": params,
    }
