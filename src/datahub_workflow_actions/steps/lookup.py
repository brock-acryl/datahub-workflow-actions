"""Lookup steps — read-only DataHub GraphQL that yields lists for later steps
to act on (§19). Each returns ``urns``, ``entities`` (urn / type / name) and
``total``; pages are fetched internally up to ``maxResults``. They run through
the executor's own DataHub client, so no extra credentials are involved, and
they run in dry-run too (reads only) so a simulated rule shows its targets."""

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional, Union

from pydantic import Field, field_validator

from datahub_workflow_actions.steps import RunContext, StepParams, step

PAGE = 500
MAX_RESULTS_CAP = 10000
StrList = Union[str, List[str]]

LOOKUP_OUTPUTS = {
    "urns": "URNs of the matching entities",
    "entities": "Matching entities as {urn, type, name}",
    "total": "Total matches reported by DataHub (may exceed the rows kept)",
}

# Name fields differ per entity; ask for the common ones.
ENTITY_FIELDS = """
    urn
    type
    ... on Dataset { name properties { name } }
    ... on Dashboard { properties { name } }
    ... on Chart { properties { name } }
    ... on DataFlow { properties { name } }
    ... on DataJob { properties { name } }
    ... on Container { properties { name } }
    ... on GlossaryTerm { properties { name } }
    ... on GlossaryNode { properties { name } }
    ... on Domain { properties { name } }
    ... on DataProduct { properties { name } }
    ... on MLModel { name }
    ... on MLModelGroup { name }
    ... on MLFeatureTable { name }
    ... on MLFeature { name }
    ... on MLPrimaryKey { name }
    ... on Tag { properties { name } }
    ... on CorpUser { username }
    ... on CorpGroup { name }
"""

SEARCH_FILTER_FIELDS = {
    # simple param → search index field
    "domain": "domains",
    "tag": "tags",
    "term": "glossaryTerms",
    "owner": "owners",
    "platform": "platform",
    "container": "container",
}


def _list(value: Optional[StrList]) -> List[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [v.strip() for v in value.split(",") if v.strip()] if not value.strip().startswith("[") else list(json.loads(value))
    return [str(v) for v in value if str(v).strip()]


def _json(value: Any, what: str) -> Any:
    if value is None or value == "":
        return None
    if isinstance(value, str):
        try:
            return json.loads(value)
        except ValueError as e:
            raise ValueError(f"{what} must be JSON: {e}") from e
    return value


def _entity_row(entity: Optional[dict]) -> Optional[dict]:
    if not entity or not entity.get("urn"):
        return None
    props = entity.get("properties") or {}
    name = props.get("name") or entity.get("name") or entity.get("username") or entity.get("urn")
    return {"urn": entity["urn"], "type": entity.get("type"), "name": name}


def _collect(rows: List[dict], total: Optional[int]) -> dict:
    entities = [row for row in (_entity_row(r) for r in rows) if row]
    return {"urns": [e["urn"] for e in entities], "entities": entities, "total": total if total is not None else len(entities)}


def _dry(reason: str) -> dict:
    return {"dryRun": True, "reason": reason, **_collect([], 0)}


def _and_group(conditions: List[dict]) -> List[dict]:
    return [{"and": conditions}] if conditions else []


def _filters_from(simple: Dict[str, Optional[StrList]], raw: Any) -> List[dict]:
    """Simple params become one AND group; raw ``filters`` (a list of
    {field, values, condition?, negated?} or a full orFilters list) are merged in."""
    conditions: List[dict] = []
    for key, field_name in SEARCH_FILTER_FIELDS.items():
        values = _list(simple.get(key))
        if values:
            conditions.append({"field": field_name, "values": values, "condition": "EQUAL"})
    extra = _json(raw, "filters") or []
    if isinstance(extra, dict):
        extra = [extra]
    if extra and all(isinstance(f, dict) and "and" in f for f in extra):
        # already orFilters: distribute the simple conditions into every branch
        return [{"and": [*conditions, *(branch.get("and") or [])]} for branch in extra]
    for f in extra:
        if not isinstance(f, dict) or not f.get("field"):
            raise ValueError("filters entries need a 'field' and 'values'")
        conditions.append(
            {
                "field": f["field"],
                "values": _list(f.get("values")),
                "condition": f.get("condition", "EQUAL"),
                **({"negated": True} if f.get("negated") else {}),
            }
        )
    return _and_group(conditions)


class LookupParams(StepParams):
    maxResults: int = Field(1000, ge=1, le=MAX_RESULTS_CAP, description="Stop after this many entities.")


class SearchParams(LookupParams):
    types: Optional[StrList] = Field(None, description="Entity types to include, e.g. DATASET, DASHBOARD. Empty = all.")
    query: str = Field("*", description="Search text; * for everything.")
    domain: Optional[StrList] = Field(None, description="Domain URN(s) the entities must belong to.")
    tag: Optional[StrList] = Field(None, description="Tag URN(s) the entities must carry.")
    term: Optional[StrList] = Field(None, description="Glossary term URN(s) the entities must carry.")
    owner: Optional[StrList] = Field(None, description="Owner URN(s).")
    platform: Optional[StrList] = Field(None, description="Data platform URN(s), e.g. urn:li:dataPlatform:snowflake.")
    container: Optional[StrList] = Field(None, description="Container URN(s).")
    filters: Optional[Any] = Field(
        None, description='Extra filters as JSON: [{"field": "origin", "values": ["PROD"]}] or a full orFilters list.'
    )

    @field_validator("types", "domain", "tag", "term", "owner", "platform", "container", mode="before")
    @classmethod
    def _blank_to_none(cls, v):
        return None if v in ("", [], None) else v


@step(
    "search",
    label="Find assets",
    description="Search DataHub for entities matching types, text and filters; later steps act on the result.",
    group="Lookup",
    params=SearchParams,
    outputs=LOOKUP_OUTPUTS,
)
def search(p: SearchParams, ctx: RunContext) -> dict:
    or_filters = _filters_from(
        {k: getattr(p, k) for k in SEARCH_FILTER_FIELDS}, p.filters
    )
    rows: List[dict] = []
    total: Optional[int] = None
    scroll_id: Optional[str] = None
    while len(rows) < p.maxResults:
        payload: Dict[str, Any] = {
            "query": p.query or "*",
            "count": min(PAGE, p.maxResults - len(rows)),
            **({"types": _list(p.types)} if _list(p.types) else {}),
            **({"orFilters": or_filters} if or_filters else {}),
            **({"scrollId": scroll_id} if scroll_id else {}),
        }
        page = ctx.query(
            f"query($input: ScrollAcrossEntitiesInput!) {{ scrollAcrossEntities(input: $input) {{ nextScrollId total searchResults {{ entity {{ {ENTITY_FIELDS} }} }} }} }}",
            {"input": payload},
            operation="scrollAcrossEntities",
        )
        if page is None:
            return _dry("no DataHub connection; search not executed") if not rows else _collect(rows, total)
        total = page.get("total", total)
        results = page.get("searchResults") or []
        rows.extend(r.get("entity") or {} for r in results)
        scroll_id = page.get("nextScrollId")
        if not scroll_id or not results:
            break
    return _collect(rows[: p.maxResults], total)


class DataProductAssetsParams(LookupParams):
    dataProduct: str = Field(description="Data product URN.")
    types: Optional[StrList] = Field(None, description="Restrict to these entity types (optional).")

    @field_validator("types", mode="before")
    @classmethod
    def _blank_to_none(cls, v):
        return None if v in ("", [], None) else v


@step(
    "data_product_assets",
    label="Assets in data product",
    description="List every asset that belongs to a data product.",
    group="Lookup",
    params=DataProductAssetsParams,
    outputs=LOOKUP_OUTPUTS,
)
def data_product_assets(p: DataProductAssetsParams, ctx: RunContext) -> dict:
    rows: List[dict] = []
    total: Optional[int] = None
    start = 0
    while len(rows) < p.maxResults:
        payload: Dict[str, Any] = {
            "query": "*",
            "start": start,
            "count": min(PAGE, p.maxResults - len(rows)),
            **({"types": _list(p.types)} if _list(p.types) else {}),
        }
        page = ctx.query(
            f"query($urn: String!, $input: SearchAcrossEntitiesInput!) {{ listDataProductAssets(urn: $urn, input: $input) {{ start count total searchResults {{ entity {{ {ENTITY_FIELDS} }} }} }} }}",
            {"urn": p.dataProduct, "input": payload},
            operation="listDataProductAssets",
        )
        if page is None:
            return _dry("no DataHub connection; lookup not executed") if not rows else _collect(rows, total)
        total = page.get("total", total)
        results = page.get("searchResults") or []
        rows.extend(r.get("entity") or {} for r in results)
        start += len(results)
        if not results or (total is not None and start >= total):
            break
    return _collect(rows[: p.maxResults], total)


def degree_values(hops: int) -> List[str]:
    """GMS's lineage degree filter accepts exactly "1", "2" and "3+" (three or more hops)."""
    values = [str(d) for d in range(1, min(hops, 2) + 1)]
    if hops >= 3:
        values.append("3+")
    return values


class LineageParams(LookupParams):
    entity: str = Field(description="Starting entity URN. Usually {{ entity.urn }}.")
    direction: str = Field("DOWNSTREAM", description="UPSTREAM or DOWNSTREAM.")
    hops: int = Field(1, ge=1, le=10, description="How many hops to follow.")
    types: Optional[StrList] = Field(None, description="Restrict to these entity types (optional).")

    @field_validator("direction")
    @classmethod
    def _direction(cls, v: str) -> str:
        v = (v or "").strip().upper()
        if v not in ("UPSTREAM", "DOWNSTREAM"):
            raise ValueError("direction must be UPSTREAM or DOWNSTREAM")
        return v

    @field_validator("types", mode="before")
    @classmethod
    def _blank_to_none(cls, v):
        return None if v in ("", [], None) else v


@step(
    "lineage",
    label="Lineage neighbours",
    description="Collect the upstream or downstream entities of an asset.",
    group="Lookup",
    params=LineageParams,
    outputs={**LOOKUP_OUTPUTS, "degrees": "Hop distance per URN"},
)
def lineage(p: LineageParams, ctx: RunContext) -> dict:
    rows: List[dict] = []
    degrees: Dict[str, int] = {}
    total: Optional[int] = None
    scroll_id: Optional[str] = None
    degree_filter = [{"and": [{"field": "degree", "values": degree_values(p.hops), "condition": "EQUAL"}]}]
    while len(rows) < p.maxResults:
        payload: Dict[str, Any] = {
            "urn": p.entity,
            "direction": p.direction,
            "query": "*",
            "count": min(PAGE, p.maxResults - len(rows)),
            "orFilters": degree_filter,
            **({"types": _list(p.types)} if _list(p.types) else {}),
            **({"scrollId": scroll_id} if scroll_id else {}),
        }
        page = ctx.query(
            f"query($input: ScrollAcrossLineageInput!) {{ scrollAcrossLineage(input: $input) {{ nextScrollId total searchResults {{ degree entity {{ {ENTITY_FIELDS} }} }} }} }}",
            {"input": payload},
            operation="scrollAcrossLineage",
        )
        if page is None:
            return {**_dry("no DataHub connection; lineage not executed"), "degrees": {}} if not rows else {**_collect(rows, total), "degrees": degrees}
        total = page.get("total", total)
        results = page.get("searchResults") or []
        for r in results:
            entity = r.get("entity") or {}
            rows.append(entity)
            if entity.get("urn") is not None and r.get("degree") is not None:
                degrees[entity["urn"]] = r["degree"]
        scroll_id = page.get("nextScrollId")
        if not scroll_id or not results:
            break
    return {**_collect(rows[: p.maxResults], total), "degrees": degrees}


class GraphqlParams(StepParams):
    query: str = Field(description="A GraphQL query (reads only — use the metadata steps to write).")
    variables: Optional[Any] = Field(None, description='Variables as JSON, e.g. {"urn": "{{ entity.urn }}"}.')
    path: Optional[str] = Field(
        None, description="Dot path into the response to expose as `value`, e.g. dataset.schemaMetadata.fields."
    )

    @field_validator("query")
    @classmethod
    def _reads_only(cls, v: str) -> str:
        if v.lstrip().lower().startswith("mutation"):
            raise ValueError("graphql lookup runs queries only; use a metadata step to mutate")
        return v


def _pluck(data: Any, path: Optional[str]) -> Any:
    if not path:
        return data
    current = data
    for part in [p for p in path.replace("[", ".").replace("]", "").split(".") if p]:
        if isinstance(current, list):
            if part.isdigit():
                idx = int(part)
                current = current[idx] if idx < len(current) else None
            else:
                current = [c.get(part) if isinstance(c, dict) else None for c in current]
        elif isinstance(current, dict):
            current = current.get(part)
        else:
            return None
    return current


@step(
    "graphql",
    label="GraphQL query",
    description="Run any read-only DataHub GraphQL query and expose the result to later steps.",
    group="Lookup",
    params=GraphqlParams,
    outputs={"data": "The query's data payload", "value": "The part of the response selected by `path` (or all of it)"},
)
def graphql(p: GraphqlParams, ctx: RunContext) -> dict:
    variables = _json(p.variables, "variables") or {}
    if not isinstance(variables, dict):
        raise ValueError("variables must be a JSON object")
    if ctx.graph is None:
        return {"dryRun": True, "reason": "no DataHub connection; query not executed", "data": None, "value": None}
    data = ctx.graph.execute_graphql(p.query, variables=variables)
    return {"data": data, "value": _pluck(data, p.path)}
