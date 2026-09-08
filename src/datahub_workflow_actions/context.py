"""Turn an EntityChangeEvent into the context document that filters and templates read.

Workflow events (an actionRequest's lifecycle) build the request-shaped document below.
Any other change event (§21) builds the event-shaped document — see ``build_event_context``:

    event      type, id, category, operation, modifier, entityType, entityUrn, parameters, time, actor
    entity     urn, type, name, platform, description, tags[], terms[], owners[], domain, parent{urn}, fieldPath
    actor      urn, username, email, name, groups[]        (who made the change)
    change     tag, term, owner, ownerType, domain, property, values, status, note, description,
               previousDescription, modificationCategory, businessAttribute, result, runId, assertee,
               incident{type,title,stage,entities}, parent, field, subject{urn,type,name}
    params     the decoded event parameters

Workflow-shaped document:

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


# ActionRequest is not an Entity in the GraphQL schema — it has its own root query.
ACTION_REQUEST_QUERY = """
query workflowActionsRequest($urn: String!) {
  actionRequest(urn: $urn) {
    urn
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
      ownership { owners { owner { ... on CorpUser { urn } ... on CorpGroup { urn } } ownershipType { urn } type } } domain { domain { urn } }
      deprecation { deprecated note decommissionTime }
      structuredProperties { properties { structuredProperty { urn } values { ... on StringValue { stringValue } ... on NumberValue { numberValue } } } } }
    ... on Dashboard { properties { name description } platform { name } tags { tags { tag { urn } } }
      glossaryTerms { terms { term { urn } } } ownership { owners { owner { ... on CorpUser { urn } ... on CorpGroup { urn } } } } domain { domain { urn } }
      deprecation { deprecated note } }
    ... on Chart { properties { name description } platform { name } tags { tags { tag { urn } } }
      glossaryTerms { terms { term { urn } } } ownership { owners { owner { ... on CorpUser { urn } ... on CorpGroup { urn } } } } domain { domain { urn } }
      deprecation { deprecated note } }
    ... on Container { properties { name description } platform { name } tags { tags { tag { urn } } }
      glossaryTerms { terms { term { urn } } } ownership { owners { owner { ... on CorpUser { urn } ... on CorpGroup { urn } } } } domain { domain { urn } }
      deprecation { deprecated note } }
    ... on DataJob { properties { name description } tags { tags { tag { urn } } }
      ownership { owners { owner { ... on CorpUser { urn } ... on CorpGroup { urn } } } } domain { domain { urn } } deprecation { deprecated note } }
    ... on DataFlow { properties { name description } tags { tags { tag { urn } } }
      ownership { owners { owner { ... on CorpUser { urn } ... on CorpGroup { urn } } } } domain { domain { urn } } deprecation { deprecated note } }
    ... on GlossaryTerm { properties { name description } ownership { owners { owner { ... on CorpUser { urn } ... on CorpGroup { urn } } } } }
    ... on GlossaryNode { properties { name description } ownership { owners { owner { ... on CorpUser { urn } ... on CorpGroup { urn } } } } }
    ... on Domain { properties { name description } ownership { owners { owner { ... on CorpUser { urn } ... on CorpGroup { urn } } } } }
    ... on DataProduct { properties { name description } ownership { owners { owner { ... on CorpUser { urn } ... on CorpGroup { urn } } } } domain { domain { urn } } }
    ... on Tag { name properties { name description } }
    ... on CorpUser { username properties { displayName email } }
    ... on CorpGroup { name properties { displayName email } }
    ... on StructuredPropertyEntity { definition { displayName qualifiedName description } }
    ... on SchemaFieldEntity { fieldPath parent { urn type ... on Dataset { name properties { name } platform { name properties { displayName } } } } }
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
        return self._query(ACTION_REQUEST_QUERY, urn, "actionRequest")

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


def _structured_properties(raw: Mapping[str, Any]) -> Dict[str, list]:
    out: Dict[str, list] = {}
    for entry in ((raw.get("structuredProperties") or {}).get("properties") or []):
        urn = (entry.get("structuredProperty") or {}).get("urn")
        if not urn:
            continue
        values = []
        for value in entry.get("values") or []:
            if value.get("stringValue") is not None:
                values.append(value["stringValue"])
            elif value.get("numberValue") is not None:
                values.append(value["numberValue"])
        out[urn] = values
    return out


def _entity_name(raw: Mapping[str, Any], urn: str, event_params: Mapping[str, Any]) -> str:
    props = raw.get("properties") or {}
    definition = raw.get("definition") or {}
    return (
        props.get("name")
        or props.get("displayName")
        or definition.get("displayName")
        or definition.get("qualifiedName")
        or raw.get("name")
        or raw.get("username")
        or raw.get("fieldPath")
        or event_params.get("entityName")
        or urn_name(urn)
    )


def _entity_doc(urn: Optional[str], event_params: Mapping[str, Any], resolver: Resolver, *, resolve_parent: bool = False) -> Dict[str, Any]:
    if not urn:
        return {}
    raw = resolver.get_entity(urn) or {}
    props = raw.get("properties") or {}
    platform = raw.get("platform") or {}
    deprecation = raw.get("deprecation") or {}
    parent_raw = raw.get("parent") if isinstance(raw.get("parent"), dict) else None
    parent_urn = (parent_raw or {}).get("urn") or event_params.get("parentUrn")
    parent: Optional[Dict[str, Any]] = None
    if parent_urn:
        if resolve_parent:
            parent = _entity_doc(parent_urn, {}, resolver)
        else:
            parent_props = (parent_raw or {}).get("properties") or {}
            parent_platform = (parent_raw or {}).get("platform") or {}
            parent = {
                "urn": parent_urn,
                "type": ((parent_raw or {}).get("type") or "").lower() or None,
                "name": parent_props.get("name") or (parent_raw or {}).get("name") or urn_name(parent_urn),
                "platform": (parent_platform.get("properties") or {}).get("displayName") or parent_platform.get("name"),
            }
    doc = {
        "urn": urn,
        "type": (raw.get("type") or event_params.get("entityType") or "").lower() or None,
        "name": _entity_name(raw, urn, event_params),
        "description": props.get("description") or (raw.get("definition") or {}).get("description"),
        "platform": (platform.get("properties") or {}).get("displayName") or platform.get("name") or (parent or {}).get("platform"),
        "tags": [t["tag"]["urn"] for t in ((raw.get("tags") or {}).get("tags") or []) if t.get("tag")],
        "terms": [t["term"]["urn"] for t in ((raw.get("glossaryTerms") or {}).get("terms") or []) if t.get("term")],
        "owners": [o["owner"]["urn"] for o in ((raw.get("ownership") or {}).get("owners") or []) if o.get("owner")],
        "domain": ((raw.get("domain") or {}).get("domain") or {}).get("urn"),
        "deprecated": bool(deprecation.get("deprecated")) if deprecation else False,
        "deprecationNote": deprecation.get("note"),
        "structuredProperties": _structured_properties(raw),
        "parent": parent,
        "fieldPath": raw.get("fieldPath") or event_params.get("fieldPath"),
        "email": (props.get("email") if raw.get("type") in ("CORP_USER", "CORP_GROUP") else None),
    }
    return doc


def parse_parameters(raw: Optional[Mapping[str, Any]]) -> Dict[str, Any]:
    """ECE parameters are a flat string map; nested values (propertyValues, tagUrns, owners, …)
    arrive JSON-encoded. Decode anything that looks like JSON, keep the rest verbatim."""
    params: Dict[str, Any] = {}
    for key, value in (raw or {}).items():
        if isinstance(value, str):
            text = value.strip()
            if text[:1] in ("{", "[") and text[-1:] in ("}", "]"):
                try:
                    value = json.loads(text)
                except ValueError:
                    pass
        params[key] = value
    return params


def event_view(event: Mapping[str, Any]) -> Dict[str, Any]:
    """The cheap, lookup-free view ``engine.event_trigger_matches`` checks on every event."""
    audit = event.get("auditStamp") or {}
    params = parse_parameters(event.get("parameters"))
    return {
        "entityType": event.get("entityType"),
        "entityUrn": event.get("entityUrn"),
        "category": event.get("category"),
        "operation": event.get("operation"),
        "modifier": event.get("modifier"),
        "parameters": params,
        "actor": params.get("actorUrn") or audit.get("actor"),
        "time": audit.get("time"),
    }


def event_id(event: Mapping[str, Any]) -> str:
    """Idempotency id. Workflow events keep the pre-§21 shape so existing state rows stay valid;
    other change events include category and modifier — the same asset gets two different tags
    at the same millisecond and both must run."""
    audit = event.get("auditStamp") or {}
    if is_workflow_lifecycle_event(event):
        return f"{event.get('entityUrn') or ''}:{event.get('operation')}:{audit.get('time')}"
    return ":".join(
        str(part if part is not None else "")
        for part in (event.get("entityUrn"), event.get("category"), event.get("operation"), event.get("modifier"), audit.get("time"))
    )


CHANGE_KEYS = (
    "tag", "term", "owner", "ownerType", "domain", "property", "values", "status", "note", "description",
    "previousDescription", "modificationCategory", "businessAttribute", "result", "runId", "assertee",
    "incident", "parent", "field", "subject",
)


def change_doc(event: Mapping[str, Any], params: Mapping[str, Any]) -> Dict[str, Any]:
    """A stable vocabulary over what changed, so templates read ``change.tag`` whatever the
    category — every key is always present (None when it does not apply)."""
    category = str(event.get("category") or "").upper()
    modifier = event.get("modifier")
    doc: Dict[str, Any] = {key: None for key in CHANGE_KEYS}
    doc.update(
        {
            "tag": params.get("tagUrn") or (modifier if category == "TAG" else None),
            "term": params.get("termUrn") or (modifier if category == "GLOSSARY_TERM" else None),
            "owner": params.get("ownerUrn") or (modifier if category == "OWNERSHIP" else None),
            "ownerType": params.get("ownerTypeUrn") or params.get("ownerType"),
            "domain": params.get("domainUrn") or (modifier if category == "DOMAIN" else None),
            "property": params.get("propertyUrn") or (modifier if category == "STRUCTURED_PROPERTY" else None),
            "values": params.get("propertyValues"),
            "status": params.get("status"),
            "note": params.get("note"),
            "description": params.get("description"),
            "previousDescription": params.get("previousDescription"),
            "modificationCategory": params.get("modificationCategory"),
            "businessAttribute": params.get("businessAttributeUrn") or (modifier if category == "BUSINESS_ATTRIBUTE" else None),
            "result": params.get("assertionResult") or params.get("runResult") or params.get("result"),
            "runId": params.get("runId") or params.get("dataProcessInstanceUrn"),
            "assertee": params.get("asserteeUrn"),
            "parent": params.get("parentUrn"),
            "field": params.get("fieldPath") or (modifier if category == "TECHNICAL_SCHEMA" else None),
            "subject": {
                "urn": event.get("entityUrn"),
                "type": (event.get("entityType") or "").lower() or None,
                "name": urn_name(event.get("entityUrn") or ""),
            },
        }
    )
    if category == "INCIDENT":
        doc["incident"] = {
            "type": params.get("type"),
            "title": params.get("title"),
            "stage": params.get("stage"),
            "entities": params.get("entities"),
        }
    return doc


def build_event_context(event: Mapping[str, Any], resolver: Resolver) -> Dict[str, Any]:
    """§21 The document an *event* rule runs against. Deliberately has no ``workflow`` /
    ``request`` / ``requester`` / ``approver`` / ``form`` keys — a template that reaches for
    them fails loudly instead of rendering an empty string."""
    view = event_view(event)
    params = view["parameters"]
    entity_urn = view.get("entityUrn")
    is_field = str(view.get("entityType") or "").lower() == "schemafield"
    entity = _entity_doc(
        entity_urn,
        {"entityType": view.get("entityType"), "parentUrn": params.get("parentUrn"), "fieldPath": params.get("fieldPath")},
        resolver,
        resolve_parent=is_field,  # column events: the dataset that owns the column, fully resolved
    )
    change = change_doc(event, params)
    if entity.get("name"):
        change["subject"]["name"] = entity["name"]
    return {
        "event": {
            "type": "EntityChangeEvent",
            "id": event_id(event),
            "category": view.get("category"),
            "operation": view.get("operation"),
            "modifier": view.get("modifier"),
            "entityType": view.get("entityType"),
            "entityUrn": entity_urn,
            "parameters": params,
            "time": view.get("time"),
            "actor": view.get("actor"),
        },
        "entity": entity,
        "actor": _user_doc(view.get("actor"), resolver),
        "change": change,
        "params": params,
    }


def build_context(event: Mapping[str, Any], resolver: Resolver) -> Dict[str, Any]:
    """``event`` is the EntityChangeEvent as a plain dict (``EntityChangeEvent.to_obj()`` plus
    parameters). Workflow lifecycle events get the request-shaped document; anything else the
    event-shaped one (§21)."""
    if not is_workflow_lifecycle_event(event):
        return build_event_context(event, resolver)
    return build_workflow_context(event, resolver)


def build_workflow_context(event: Mapping[str, Any], resolver: Resolver) -> Dict[str, Any]:
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
