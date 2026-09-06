"""The rules contract (schemaVersion 1). Field names are camelCase to match
the MFE builder. ``rules_json_schema()`` is the published schema the MFE
vendors; ``load_rules()`` accepts the recipe root, a bare config, or a list."""

from __future__ import annotations

import json
from typing import Any, Dict, List, Literal, Optional, Union

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


class Trigger(StrictModel):
    """Which lifecycle event fires the rule."""

    operation: LifecycleOperation
    result: Optional[LifecycleResult] = Field(
        None, description="COMPLETED: the request outcome. MODIFY: the step decision."
    )
    stepId: Optional[str] = Field(None, description="MODIFY only: restrict to one approval step.")

    @model_validator(mode="after")
    def _check(self) -> "Trigger":
        if self.operation == "COMPLETED" and self.result is None:
            raise ValueError("a COMPLETED trigger needs a result (ACCEPTED / REJECTED / CANCELLED)")
        if self.stepId and self.operation != "MODIFY":
            raise ValueError("stepId only applies to MODIFY (step decided) triggers")
        if self.operation in ("CREATE", "PENDING") and self.result is not None:
            raise ValueError(f"a {self.operation} trigger has no result")
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
    idempotencyKey: Optional[str] = Field(
        None, description="Template; defaults to event id + rule id + step id (+ item index)."
    )


class Rule(StrictModel):
    id: str = Field(min_length=1)
    name: Optional[str] = None
    description: Optional[str] = None
    workflowUrn: str = Field(min_length=1)
    enabled: bool = True
    on: Trigger
    when: Optional[FilterGroup] = None
    steps: List[Step] = Field(default_factory=list)
    onError: StepErrorPolicy = Field("fail", description="Default error policy for steps.")

    @model_validator(mode="after")
    def _check(self) -> "Rule":
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


def rules_json_schema() -> Dict[str, Any]:
    schema = RulesConfig.model_json_schema()
    schema["$schema"] = "https://json-schema.org/draft/2020-12/schema"
    schema["$id"] = f"https://acryl.io/schemas/datahub-workflow-actions/v{SCHEMA_VERSION}.json"
    schema["title"] = "DataHub workflow-actions rules"
    return schema
