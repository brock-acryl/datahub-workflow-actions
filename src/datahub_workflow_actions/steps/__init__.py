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
    connections: Dict[str, str] = field(default_factory=dict)  # name → SQLAlchemy URL (plain `url` connections)
    connection_resolver: Any = None  # connections.ConnectionResolver — ingestionSource / fromEntity kinds need the graph

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

    def query(self, query: str, variables: dict, *, operation: str) -> Any:
        """Runs a read-only query and returns its ``operation`` payload. Lookups
        run even in dry-run mode (they only read) so a simulated rule shows what
        it would act on; without a graph (offline simulate) they return None."""
        if self.graph is None:
            return None
        result = self.graph.execute_graphql(query, variables=variables)
        return result.get(operation) if isinstance(result, dict) else result


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
    # The param that accepts a list (e.g. "entity"): with forEach the engine merges items into one call per chunk.
    bulk_param: Optional[str] = None

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
            "bulkParam": self.bulk_param,
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
    bulk_param: Optional[str] = None,
):
    def decorator(fn: Callable[[Any, RunContext], dict]) -> Callable[[Any, RunContext], dict]:
        REGISTRY[type_] = StepDefinition(
            type_, label, description, group, params, fn, outputs or {}, validate_template, bulk_param
        )
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
    from datahub_workflow_actions.steps import integration, lookup, metadata, sql  # noqa: F401
