"""Workflow steps: raise a new request in another (or the same) workflow.

The fan-out pattern — "one request raises one request per selected data product" — is a
`raise_workflow` step with `forEach: form.<multi-value field>` and `entity: "{{ item }}"`; the
engine runs the step once per item and each run creates one request."""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Union

from pydantic import Field

from datahub_workflow_actions.steps import RunContext, StepParams, step

WORKFLOW_URN_PREFIX = "urn:li:actionWorkflow:"
REQUEST_ID_RE = re.compile(r"^[a-zA-Z0-9_-]{1,200}$")

FieldValues = Union[str, dict, list, None]


class RaiseWorkflowParams(StepParams):
    workflow: str = Field(description="URN of the workflow to raise a request in.")
    entity: Optional[str] = Field(
        None, description="Asset the new request is about (entity URN). With for-each over a list of assets, {{ item }}."
    )
    fields: FieldValues = Field(
        None,
        description='Form answers for the new request by field id, e.g. {"f_reason": "{{ form.f_reason }}", "f_owner": "{{ requester.urn }}"}. A list fills a multi-value field.',
    )
    description: Optional[str] = Field(None, description="Description shown to the new request's reviewers.")
    requestId: Optional[str] = Field(
        None,
        description="Stable id for the new request (letters, digits, - and _) so a replay does not raise it twice, e.g. {{ request.id }}-{{ index }}.",
    )


def _property_value(value: Any) -> Dict[str, Any]:
    if isinstance(value, bool):
        return {"stringValue": "true" if value else "false"}
    if isinstance(value, (int, float)):
        return {"numberValue": float(value)}
    return {"stringValue": str(value)}


def _field_inputs(fields: FieldValues) -> List[Dict[str, Any]]:
    if fields in (None, ""):
        return []
    if isinstance(fields, str):
        import json

        try:
            fields = json.loads(fields)
        except ValueError as e:
            raise ValueError(f"fields must be a JSON object of field id → value: {e}") from e
    if not isinstance(fields, dict):
        raise ValueError("fields must be an object of field id → value")
    out: List[Dict[str, Any]] = []
    for field_id, value in fields.items():
        if value is None or value == "" or value == []:
            continue  # unanswered optional field
        values = value if isinstance(value, list) else [value]
        out.append({"id": str(field_id), "values": [_property_value(v) for v in values]})
    return out


def _validate_template(raw: Dict[str, Any]) -> List[str]:
    problems: List[str] = []
    workflow = raw.get("workflow")
    if isinstance(workflow, str) and "{{" not in workflow and not workflow.startswith(WORKFLOW_URN_PREFIX):
        problems.append(f"workflow must be a workflow URN ({WORKFLOW_URN_PREFIX}…), got '{workflow}'")
    fields = raw.get("fields")
    if isinstance(fields, str) and fields.strip() and "{{" not in fields:
        import json

        try:
            if not isinstance(json.loads(fields), dict):
                problems.append("fields must be a JSON object of field id → value")
        except ValueError:
            problems.append("fields must be a JSON object of field id → value")
    return problems


RAISE_MUTATION = """mutation($input: CreateActionWorkflowFormRequestInput!) {
  createActionWorkflowFormRequestV2(input: $input) { request { urn } fieldErrors { fieldId errorMessage } }
}"""


@step(
    "raise_workflow",
    label="Raise workflow request",
    description="Start a new request in a workflow, filled in from this one — once, or once per item.",
    group="Workflows",
    params=RaiseWorkflowParams,
    outputs={"urn": "The new request's URN", "id": "The new request's id"},
    validate_template=_validate_template,
)
def raise_workflow(p: RaiseWorkflowParams, ctx: RunContext) -> dict:
    if not p.workflow.startswith(WORKFLOW_URN_PREFIX):
        raise ValueError(f"workflow must be a workflow URN ({WORKFLOW_URN_PREFIX}…), got '{p.workflow}'")
    request_id = (p.requestId or "").strip() or None
    if request_id and not REQUEST_ID_RE.match(request_id):
        raise ValueError(f"requestId '{request_id}' must be 1–200 characters of letters, digits, - or _")
    input_: Dict[str, Any] = {"workflowUrn": p.workflow, "fields": _field_inputs(p.fields)}
    if p.entity:
        input_["entityUrn"] = p.entity
    if p.description:
        input_["description"] = p.description
    if request_id:
        input_["id"] = request_id
    out = ctx.graphql(RAISE_MUTATION, {"input": input_}, mutation="createActionWorkflowFormRequestV2")
    if out.get("dryRun"):
        return {"urn": f"urn:li:actionRequest:{request_id}" if request_id else None, "id": request_id, "dryRun": True, "input": input_}
    result = out.get("result") or {}
    errors = result.get("fieldErrors") or []
    if errors:
        detail = "; ".join(f"{e.get('fieldId')}: {e.get('errorMessage')}" for e in errors)
        raise RuntimeError(f"workflow {p.workflow} rejected the request: {detail}")
    urn = (result.get("request") or {}).get("urn")
    if not urn:
        raise RuntimeError(f"workflow {p.workflow} returned no request urn")
    return {"urn": urn, "id": urn.rsplit(":", 1)[-1]}
