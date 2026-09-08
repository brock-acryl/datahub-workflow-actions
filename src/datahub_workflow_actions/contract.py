"""The rules contract (schemaVersion 1). Field names are camelCase to match
the MFE builder. ``rules_json_schema()`` is the published schema the MFE
vendors; ``load_rules()`` accepts the recipe root, a bare config, or a list."""

from __future__ import annotations

import json
from typing import Annotated, Any, Dict, List, Literal, Optional, Union

from pydantic import BaseModel, ConfigDict, Field, model_validator

SCHEMA_VERSION = 1
SUPPORTED_SCHEMA_VERSIONS = {1}
SOURCE_TYPE = "datahub-workflow-actions"

LifecycleOperation = Literal["CREATE", "PENDING", "MODIFY", "COMPLETED"]
LifecycleResult = Literal["ACCEPTED", "REJECTED", "CANCELLED"]
FilterCondition = Literal[
    "EQUAL", "CONTAIN", "START_WITH", "END_WITH", "EXISTS", "IN", "GREATER_THAN", "LESS_THAN", "MATCHES"
]
StepErrorPolicy = Literal["fail", "continue", "stop"]


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class WorkflowTrigger(StrictModel):
    """Fires on a workflow request's lifecycle (created / step decided / completed)."""

    type: Literal["workflow"] = "workflow"
    operation: LifecycleOperation
    result: Optional[LifecycleResult] = Field(
        None, description="COMPLETED: the request outcome. MODIFY: the step decision."
    )
    stepId: Optional[str] = Field(None, description="MODIFY only: restrict to one approval step.")

    @model_validator(mode="after")
    def _check(self) -> "WorkflowTrigger":
        if self.operation == "COMPLETED" and self.result is None:
            raise ValueError("a COMPLETED trigger needs a result (ACCEPTED / REJECTED / CANCELLED)")
        if self.stepId and self.operation != "MODIFY":
            raise ValueError("stepId only applies to MODIFY (step decided) triggers")
        if self.operation in ("CREATE", "PENDING") and self.result is not None:
            raise ValueError(f"a {self.operation} trigger has no result")
        return self


# Back-compat name: the workflow trigger was the only trigger before §21.
Trigger = WorkflowTrigger


class ValueMatch(StrictModel):
    """A match on a single value (the event's ``modifier``): same conditions as a Filter, no path."""

    condition: FilterCondition = "EQUAL"
    values: List[str] = Field(default_factory=list)
    negated: bool = False
    caseInsensitive: bool = False

    @model_validator(mode="after")
    def _check(self) -> "ValueMatch":
        if self.condition != "EXISTS" and not any(v.strip() for v in self.values):
            raise ValueError("a modifier match needs at least one value")
        return self


class Filter(StrictModel):
    """One condition over a dotted path into the context document."""

    field: str = Field(min_length=1, description="Dotted path, e.g. form.field_abc, entity.type, requester.groups")
    condition: FilterCondition = "EQUAL"
    values: List[str] = Field(default_factory=list)
    negated: bool = False
    caseInsensitive: bool = False

    @model_validator(mode="after")
    def _check(self) -> "Filter":
        if self.condition != "EXISTS" and not any(v.strip() for v in self.values):
            raise ValueError(f"condition on '{self.field}' needs at least one value")
        return self


class EventTrigger(StrictModel):
    """Fires on any DataHub EntityChangeEvent (§21): a category of change, optionally narrowed by
    operation, entity type, the event's ``modifier`` (the tag / term / owner / domain / property
    urn) and matches on its ``parameters``. Categories and operations are free strings — GMS adds
    them without a release of this package; see ``KNOWN_TRIGGERS`` for the published vocabulary."""

    type: Literal["event"]
    category: str = Field(min_length=1, description="TAG, GLOSSARY_TERM, OWNERSHIP, DOMAIN, DEPRECATION, LIFECYCLE, STRUCTURED_PROPERTY, TECHNICAL_SCHEMA, DOCUMENTATION, BUSINESS_ATTRIBUTE, RUN, INCIDENT, …")
    operations: List[str] = Field(default_factory=list, description="ADD, REMOVE, MODIFY, CREATE, HARD_DELETE, SOFT_DELETE, REINSTATE, STARTED, COMPLETED, ACTIVE, RESOLVED, … Empty = any.")
    entityTypes: List[str] = Field(default_factory=list, description="Entity names as the event carries them (dataset, schemaField, container, …). Empty = any.")
    modifier: Optional[ValueMatch] = Field(None, description="Match on the event's modifier — the tag / term / owner / domain / property urn.")
    parameters: List[Filter] = Field(default_factory=list, description="All must hold; `field` is a dotted path into the event's parameters (e.g. status, modificationCategory).")
    ignoreOwnChanges: bool = Field(True, description="Skip events caused by this engine's own steps, so a rule cannot re-trigger itself.")

    @model_validator(mode="after")
    def _normalise(self) -> "EventTrigger":
        self.category = self.category.strip().upper()
        self.operations = [op.strip().upper() for op in self.operations if op.strip()]
        self.entityTypes = [t.strip() for t in self.entityTypes if t.strip()]
        return self


RuleTrigger = Annotated[Union[WorkflowTrigger, EventTrigger], Field(discriminator="type")]


class FilterGroup(StrictModel):
    """AND/OR over conditions; groups nest (``(A and B) or C``)."""

    operator: Literal["AND", "OR"]
    filters: List["FilterNode"] = Field(default_factory=list)


FilterNode = Union[FilterGroup, Filter]
FilterGroup.model_rebuild()


class RetryPolicy(StrictModel):
    attempts: int = Field(1, ge=1, le=20, description="Total attempts including the first.")
    backoff: Literal["fixed", "exponential"] = "exponential"
    delaySeconds: float = Field(1.0, ge=0)
    maxDelaySeconds: float = Field(60.0, ge=0)


class BatchPolicy(StrictModel):
    """How a forEach fan-out is issued when the step accepts a list (§19)."""

    mode: Literal["auto", "items"] = Field(
        "auto",
        description="auto: items whose other params match are merged into one call per chunk; items: one call per item.",
    )
    size: int = Field(100, ge=1, le=1000, description="Max items per merged call.")


class Step(StrictModel):
    id: str = Field(min_length=1)
    type: str = Field(min_length=1, description="A step type from the catalog (add_tag, webhook, …).")
    params: Dict[str, Any] = Field(default_factory=dict, description="Templated values ({{ path }}).")
    description: Optional[str] = None
    enabled: bool = True
    when: Optional[FilterGroup] = None
    onError: Optional[StepErrorPolicy] = Field(None, description="Overrides the rule default (fail).")
    retry: Optional[RetryPolicy] = None
    timeoutSeconds: Optional[float] = Field(None, gt=0)
    forEach: Optional[str] = Field(
        None, description="Path or template yielding a list; the step runs once per element as `item`."
    )
    itemWhen: Optional[FilterGroup] = Field(
        None, description="With forEach: conditions evaluated per element (`item`, `index` in scope); non-matching items are skipped."
    )
    batch: Optional[BatchPolicy] = Field(
        None, description="With forEach on a step that accepts a list: merge items into bulk calls (default auto, 100 per call)."
    )
    idempotencyKey: Optional[str] = Field(
        None, description="Template; defaults to event id + rule id + step id (+ item index)."
    )


class Rule(StrictModel):
    id: str = Field(min_length=1)
    name: Optional[str] = None
    description: Optional[str] = None
    workflowUrn: Optional[str] = Field(None, min_length=1, description="Workflow triggers only.")
    enabled: bool = True
    on: RuleTrigger
    when: Optional[FilterGroup] = None
    steps: List[Step] = Field(default_factory=list)
    onError: StepErrorPolicy = Field("fail", description="Default error policy for steps.")

    @model_validator(mode="before")
    @classmethod
    def _default_trigger_type(cls, data: Any) -> Any:
        # Recipes written before §21 have no `type` on the trigger: they are workflow triggers.
        if isinstance(data, dict) and isinstance(data.get("on"), dict) and "type" not in data["on"]:
            data = {**data, "on": {**data["on"], "type": "workflow"}}
        return data

    @property
    def trigger_type(self) -> str:
        return self.on.type

    @model_validator(mode="after")
    def _check(self) -> "Rule":
        if self.on.type == "workflow" and not self.workflowUrn:
            raise ValueError(f"rule '{self.id}': a workflow trigger needs workflowUrn")
        if self.on.type != "workflow" and self.workflowUrn:
            raise ValueError(f"rule '{self.id}': {self.on.type} rules are scoped by entityTypes/when, not workflowUrn")
        seen = set()
        for step in self.steps:
            if step.id in seen:
                raise ValueError(f"rule '{self.id}': duplicate step id '{step.id}'")
            seen.add(step.id)
        return self


class RulesConfig(BaseModel):
    """``source.config`` of the recipe. Other keys in the block are opaque."""

    model_config = ConfigDict(extra="ignore")

    schemaVersion: int = SCHEMA_VERSION
    rules: List[Rule] = Field(default_factory=list)

    @model_validator(mode="after")
    def _check(self) -> "RulesConfig":
        if self.schemaVersion not in SUPPORTED_SCHEMA_VERSIONS:
            raise ValueError(
                f"unsupported schemaVersion {self.schemaVersion}; this build supports {sorted(SUPPORTED_SCHEMA_VERSIONS)}"
            )
        seen = set()
        for rule in self.rules:
            if rule.id in seen:
                raise ValueError(f"duplicate rule id '{rule.id}'")
            seen.add(rule.id)
        return self

    def rules_for(self, workflow_urn: str) -> List[Rule]:
        return [rule for rule in self.rules if rule.workflowUrn == workflow_urn]

    def workflow_rules(self) -> List[Rule]:
        return [rule for rule in self.rules if rule.on.type == "workflow"]

    def event_rules(self) -> List[Rule]:
        return [rule for rule in self.rules if rule.on.type == "event"]


# The EntityChangeEvent vocabulary GMS emits today (verified against the DataHub fork's
# EntityChangeEventGenerators). Published as contracts/triggers.json so the MFE's presets and
# the `validate` command share one source of truth. Free strings in the contract; this is advice.
KNOWN_TRIGGERS: Dict[str, Dict[str, Any]] = {
    "TAG": {"label": "Tags", "operations": ["ADD", "REMOVE"], "modifier": "tag", "parameterKeys": ["tagUrn", "context", "sourceDetails", "parentUrn", "fieldPath"], "changeKeys": ["tag", "parent", "field"], "notes": "Field tags arrive with entityType schemaField and parentUrn = the dataset."},
    "GLOSSARY_TERM": {"label": "Glossary terms", "operations": ["ADD", "REMOVE"], "modifier": "term", "parameterKeys": ["termUrn", "context", "sourceDetails", "parentUrn", "fieldPath"], "changeKeys": ["term", "parent", "field"]},
    "OWNERSHIP": {"label": "Owners", "operations": ["ADD", "REMOVE"], "modifier": "owner", "parameterKeys": ["ownerUrn", "ownerType", "ownerTypeUrn", "sourceDetails"], "changeKeys": ["owner", "ownerType", "ownerTypeUrn"]},
    "DOMAIN": {"label": "Domains", "operations": ["ADD", "REMOVE"], "modifier": "domain", "parameterKeys": ["domainUrn", "context", "sourceDetails"], "changeKeys": ["domain"]},
    "DEPRECATION": {"label": "Deprecation", "operations": ["MODIFY"], "modifier": None, "parameterKeys": ["status", "note", "timestamp"], "changeKeys": ["status", "note"], "notes": "parameters.status is DEPRECATED or ACTIVE."},
    "LIFECYCLE": {"label": "Asset lifecycle", "operations": ["CREATE", "HARD_DELETE", "SOFT_DELETE", "REINSTATE", "PENDING", "COMPLETED", "MODIFY"], "modifier": None, "parameterKeys": ["actionRequestType", "resourceUrn", "workflowUrn", "result"], "changeKeys": [], "notes": "CREATE/HARD_DELETE for dataset, container, chart, dashboard, dataFlow, dataJob, domain, tag, glossaryTerm, corpGroup; SOFT_DELETE/REINSTATE for any asset; actionRequest lifecycle for proposals and workflow requests."},
    "STRUCTURED_PROPERTY": {"label": "Structured properties", "operations": ["ADD", "REMOVE", "MODIFY"], "modifier": "property", "parameterKeys": ["propertyUrn", "propertyValues", "sourceDetails"], "changeKeys": ["property", "values"]},
    "TECHNICAL_SCHEMA": {"label": "Schema", "operations": ["ADD", "REMOVE", "MODIFY"], "modifier": "schemaField", "parameterKeys": ["fieldPath", "fieldUrn", "nullable", "modificationCategory"], "changeKeys": ["field", "fieldUrn", "modificationCategory", "nullable"], "notes": "modificationCategory is RENAME, TYPE_CHANGE or OTHER."},
    "DOCUMENTATION": {"label": "Documentation", "operations": ["ADD", "REMOVE", "MODIFY"], "modifier": None, "parameterKeys": ["description", "previousDescription", "fieldPath", "parentUrn"], "changeKeys": ["description", "previousDescription", "field", "parent"]},
    "BUSINESS_ATTRIBUTE": {"label": "Business attributes", "operations": ["ADD", "REMOVE"], "modifier": "businessAttribute", "parameterKeys": ["businessAttributeUrn"], "changeKeys": ["businessAttribute"]},
    "RUN": {"label": "Runs", "operations": ["STARTED", "COMPLETED"], "modifier": None, "parameterKeys": ["runResult", "runId", "asserteeUrn", "assertionResult", "attempt", "parentInstanceUrn", "dataFlowUrn", "dataJobUrn"], "changeKeys": ["result", "runId", "assertee"], "notes": "Assertions emit COMPLETED with assertionResult SUCCESS / FAILURE / ERROR; pipeline runs (dataProcessInstance) emit STARTED and COMPLETED."},
    "INCIDENT": {"label": "Incidents", "operations": ["ACTIVE", "RESOLVED"], "modifier": None, "parameterKeys": ["entities", "type", "title", "description", "stage", "message"], "changeKeys": ["incident"], "notes": "DataHub Cloud only."},
}

KNOWN_ENTITY_TYPES: List[str] = [
    "dataset", "schemaField", "chart", "dashboard", "dataFlow", "dataJob", "container", "glossaryTerm", "glossaryNode",
    "domain", "dataProduct", "tag", "corpuser", "corpGroup", "mlModel", "mlModelGroup", "mlFeature", "mlFeatureTable",
    "mlPrimaryKey", "notebook", "assertion", "incident", "dataProcessInstance", "actionRequest", "businessAttribute",
]


def triggers_catalog() -> Dict[str, Any]:
    return {"categories": KNOWN_TRIGGERS, "entityTypes": KNOWN_ENTITY_TYPES}


def load_rules(obj: Any) -> RulesConfig:
    """Accepts the recipe root (``{source: {config: {...}}}``), a bare config
    (``{schemaVersion, rules}``), or a bare list of rules."""
    if isinstance(obj, str):
        obj = json.loads(obj)
    if isinstance(obj, list):
        return RulesConfig(rules=obj)
    if not isinstance(obj, dict):
        raise ValueError("rules must be an object or a list")
    source = obj.get("source")
    if isinstance(source, dict) and isinstance(source.get("config"), dict):
        return RulesConfig.model_validate(source["config"])
    if "rules" in obj or "schemaVersion" in obj:
        return RulesConfig.model_validate(obj)
    raise ValueError("no rules found: expected source.config.rules, {rules: [...]}, or a list")


_NUMERIC_BOUNDS = {"minimum", "maximum", "exclusiveMinimum", "exclusiveMaximum", "multipleOf"}


def strip_auto_titles(node: Any) -> Any:
    """Normalise pydantic's JSON Schema so the published contract does not drift
    between pydantic versions without a real change: drop auto-generated
    ``title`` keys ("OnError" vs "Onerror"), drop ``additionalProperties: true``
    (the JSON Schema default; 2.13 started emitting it for free-form dicts) and
    write whole-number bounds as integers (``0.0`` vs ``0``). ``$defs`` keys keep
    their model names."""
    if isinstance(node, dict):
        out: Dict[str, Any] = {}
        for key, value in node.items():
            if key == "title":
                continue
            if key == "additionalProperties" and value is True:
                continue
            if key in _NUMERIC_BOUNDS and isinstance(value, float) and value.is_integer():
                out[key] = int(value)
                continue
            out[key] = strip_auto_titles(value)
        return out
    if isinstance(node, list):
        return [strip_auto_titles(v) for v in node]
    return node


def rules_json_schema() -> Dict[str, Any]:
    schema = strip_auto_titles(RulesConfig.model_json_schema())
    schema["$schema"] = "https://json-schema.org/draft/2020-12/schema"
    schema["$id"] = f"https://acryl.io/schemas/datahub-workflow-actions/v{SCHEMA_VERSION}.json"
    schema["title"] = "DataHub workflow-actions rules"
    return schema
