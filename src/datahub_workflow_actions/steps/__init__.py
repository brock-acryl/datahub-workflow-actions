"""Step registry. A step is a typed params model plus a ``run`` function;
``catalog()`` describes every step (params, outputs) so the MFE picker and the
JSON Schema can be generated from the same source."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Type

import requests
from pydantic import BaseModel, ConfigDict

logger = logging.getLogger("datahub_workflow_actions.steps")


class StepParams(BaseModel):
    model_config = ConfigDict(extra="forbid")


@dataclass
class RunContext:
    graph: Any = None  # DataHubGraph (execute_graphql) or None in simulate
    http: Optional[requests.Session] = None
    dry_run: bool = False
    timeout: Optional[float] = None
    sleep: Callable[[float], None] = field(default=lambda s: __import__("time").sleep(s))
    env: Dict[str, str] = field(default_factory=lambda: dict(__import__("os").environ))
    context: Dict[str, Any] = field(default_factory=dict)  # the event context document
    connections: Dict[str, str] = field(default_factory=dict)  # name → SQLAlchemy URL (from the action config)

    def session(self) -> requests.Session:
        if self.http is None:
            self.http = requests.Session()
        return self.http

    def graphql(self, query: str, variables: dict, *, mutation: str) -> dict:
        """Runs a mutation, or describes it in dry-run mode."""
        if self.dry_run or self.graph is None:
            return {"dryRun": True, "mutation": mutation, "variables": variables}
        result = self.graph.execute_graphql(query, variables=variables)
        return {"mutation": mutation, "result": result.get(mutation) if isinstance(result, dict) else result}


@dataclass
class StepDefinition:
    type: str
    label: str
    description: str
    group: str
    params: Type[StepParams]
    run: Callable[[Any, RunContext], dict]
    outputs: Dict[str, str] = field(default_factory=dict)
    # Inspects RAW (unrendered) params; returns problems. Used by the engine before rendering and by `validate`.
    validate_template: Optional[Callable[[Dict[str, Any]], List[str]]] = None

    def describe(self) -> dict:
        schema = self.params.model_json_schema()
        return {
            "type": self.type,
            "label": self.label,
            "description": self.description,
            "group": self.group,
            "params": schema.get("properties", {}),
            "required": schema.get("required", []),
            "outputs": self.outputs,
        }


REGISTRY: Dict[str, StepDefinition] = {}


def step(
    type_: str,
    *,
    label: str,
    description: str,
    group: str,
    params: Type[StepParams],
    outputs: Optional[Dict[str, str]] = None,
    validate_template: Optional[Callable[[Dict[str, Any]], List[str]]] = None,
):
    def decorator(fn: Callable[[Any, RunContext], dict]) -> Callable[[Any, RunContext], dict]:
        REGISTRY[type_] = StepDefinition(type_, label, description, group, params, fn, outputs or {}, validate_template)
        return fn

    return decorator


def get_step(type_: str) -> StepDefinition:
    _ensure_loaded()
    if type_ not in REGISTRY:
        raise KeyError(f"unknown step type '{type_}'; known: {sorted(REGISTRY)}")
    return REGISTRY[type_]


def known_step_types() -> List[str]:
    _ensure_loaded()
    return sorted(REGISTRY)


def catalog() -> List[dict]:
    """Public step types only — `_`-prefixed registrations are test/private helpers."""
    _ensure_loaded()
    return [REGISTRY[t].describe() for t in sorted(REGISTRY) if not t.startswith("_")]


def _ensure_loaded() -> None:
    # Import the built-in step modules once; they register themselves.
    from datahub_workflow_actions.steps import integration, metadata, sql  # noqa: F401
