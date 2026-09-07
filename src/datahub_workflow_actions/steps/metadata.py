"""Metadata steps — DataHub GraphQL mutations, run as the executor's actor.
Every step takes a list of entities (``bulk_param="entity"``) and uses GMS batch
mutations where they exist, chunked; ``update_description`` and
``set_structured_property`` have no batch API and loop per entity (§19)."""

from __future__ import annotations

from typing import List, Optional, Union

from pydantic import Field

from datahub_workflow_actions.steps import RunContext, StepParams, step

ResourceList = Union[str, List[str]]


def split_list(value: str) -> List[str]:
    """Split on commas outside parentheses — dataset urns contain commas."""
    parts: List[str] = []
    depth = 0
    current = []
    for char in value:
        if char == "(":
            depth += 1
        elif char == ")":
            depth = max(0, depth - 1)
        if char == "," and depth == 0:
            parts.append("".join(current))
            current = []
        else:
            current.append(char)
    parts.append("".join(current))
    return [p.strip() for p in parts if p.strip()]


def _urns(value: ResourceList) -> List[str]:
    if isinstance(value, str):
        return split_list(value)
    return [str(v) for v in value if str(v).strip()]


def _resources(entity: ResourceList) -> List[dict]:
    return [{"resourceUrn": urn} for urn in _urns(entity)]


# GMS batch mutations take the whole list; keep each call to a sane size (§19).
BATCH_CHUNK = 200


def _chunks(items: List, size: int = BATCH_CHUNK) -> List[List]:
    return [items[i : i + size] for i in range(0, len(items), size)] or [[]]


def _batched(ctx: RunContext, query: str, mutation: str, resources: List, make_input) -> dict:
    """Runs ``mutation`` once per chunk of ``resources``; a single chunk keeps the plain result shape."""
    results = [ctx.graphql(query, {"input": make_input(chunk)}, mutation=mutation) for chunk in _chunks(resources)]
    if len(results) == 1:
        return results[0]
    return {"mutation": mutation, "chunks": len(results), "results": results}


class EntityParams(StepParams):
    entity: ResourceList = Field(description="Entity URN (or a list / comma-separated list). Usually {{ entity.urn }}.")


class TagParams(EntityParams):
    tag: ResourceList = Field(description="Tag URN(s).")


class TermParams(EntityParams):
    term: ResourceList = Field(description="Glossary term URN(s).")


class OwnerParams(EntityParams):
    owner: ResourceList = Field(description="User or group URN(s). Usually {{ requester.urn }}.")
    ownershipType: str = Field("TECHNICAL_OWNER", description="TECHNICAL_OWNER, BUSINESS_OWNER, DATA_STEWARD, NONE, or a custom ownership-type URN.")


class RemoveOwnerParams(EntityParams):
    owner: ResourceList
    ownershipType: Optional[str] = Field(None, description="Custom ownership-type URN to remove (optional).")


class DomainParams(EntityParams):
    domain: str = Field(description="Domain URN.")


class DataProductParams(EntityParams):
    dataProduct: str = Field(description="Data product URN.")


class StructuredPropertyParams(EntityParams):
    property: str = Field(description="Structured property URN.")
    values: ResourceList = Field(description="Value(s); numbers are sent as numbers.")


class DeprecateParams(EntityParams):
    note: Optional[str] = Field(None, description="Shown on the deprecation banner.")
    replacement: Optional[str] = Field(None, description="Replacement entity URN.")
    decommissionTime: Optional[int] = Field(None, description="Epoch millis.")


class DescriptionParams(EntityParams):
    description: str


def _owner_entity_type(urn: str) -> str:
    return "CORP_GROUP" if urn.startswith("urn:li:corpGroup:") else "CORP_USER"


@step("add_tag", label="Add tag", description="Attach a tag to an entity.", group="Metadata", params=TagParams, bulk_param="entity")
def add_tag(p: TagParams, ctx: RunContext) -> dict:
    return _batched(
        ctx, "mutation($input: BatchAddTagsInput!) { batchAddTags(input: $input) }", "batchAddTags",
        _resources(p.entity), lambda chunk: {"tagUrns": _urns(p.tag), "resources": chunk},
    )


@step("remove_tag", label="Remove tag", description="Remove a tag from an entity.", group="Metadata", params=TagParams, bulk_param="entity")
def remove_tag(p: TagParams, ctx: RunContext) -> dict:
    return _batched(
        ctx, "mutation($input: BatchRemoveTagsInput!) { batchRemoveTags(input: $input) }", "batchRemoveTags",
        _resources(p.entity), lambda chunk: {"tagUrns": _urns(p.tag), "resources": chunk},
    )


@step("add_term", label="Add glossary term", description="Attach a glossary term to an entity.", group="Metadata", params=TermParams, bulk_param="entity")
def add_term(p: TermParams, ctx: RunContext) -> dict:
    return _batched(
        ctx, "mutation($input: BatchAddTermsInput!) { batchAddTerms(input: $input) }", "batchAddTerms",
        _resources(p.entity), lambda chunk: {"termUrns": _urns(p.term), "resources": chunk},
    )


@step("remove_term", label="Remove glossary term", description="Remove a glossary term from an entity.", group="Metadata", params=TermParams, bulk_param="entity")
def remove_term(p: TermParams, ctx: RunContext) -> dict:
    return _batched(
        ctx, "mutation($input: BatchRemoveTermsInput!) { batchRemoveTerms(input: $input) }", "batchRemoveTerms",
        _resources(p.entity), lambda chunk: {"termUrns": _urns(p.term), "resources": chunk},
    )


@step("add_owner", label="Add owner", description="Add a user or group as an owner.", group="Metadata", params=OwnerParams, bulk_param="entity")
def add_owner(p: OwnerParams, ctx: RunContext) -> dict:
    is_custom = p.ownershipType.startswith("urn:")
    owners = [
        {
            "ownerUrn": urn,
            "ownerEntityType": _owner_entity_type(urn),
            **({"ownershipTypeUrn": p.ownershipType} if is_custom else {"type": p.ownershipType}),
        }
        for urn in _urns(p.owner)
    ]
    return _batched(
        ctx, "mutation($input: BatchAddOwnersInput!) { batchAddOwners(input: $input) }", "batchAddOwners",
        _resources(p.entity), lambda chunk: {"owners": owners, "resources": chunk},
    )


@step("remove_owner", label="Remove owner", description="Remove a user or group from the owners.", group="Metadata", params=RemoveOwnerParams, bulk_param="entity")
def remove_owner(p: RemoveOwnerParams, ctx: RunContext) -> dict:
    extra = {"ownershipTypeUrn": p.ownershipType} if p.ownershipType else {}
    return _batched(
        ctx, "mutation($input: BatchRemoveOwnersInput!) { batchRemoveOwners(input: $input) }", "batchRemoveOwners",
        _resources(p.entity), lambda chunk: {"ownerUrns": _urns(p.owner), "resources": chunk, **extra},
    )


@step("set_domain", label="Set domain", description="Move an entity into a domain.", group="Metadata", params=DomainParams, bulk_param="entity")
def set_domain(p: DomainParams, ctx: RunContext) -> dict:
    return _batched(
        ctx, "mutation($input: BatchSetDomainInput!) { batchSetDomain(input: $input) }", "batchSetDomain",
        _resources(p.entity), lambda chunk: {"domainUrn": p.domain, "resources": chunk},
    )


@step("clear_domain", label="Clear domain", description="Remove an entity from its domain.", group="Metadata", params=EntityParams, bulk_param="entity")
def clear_domain(p: EntityParams, ctx: RunContext) -> dict:
    # batchSetDomain with no domain clears it — one call for the whole list.
    return _batched(
        ctx, "mutation($input: BatchSetDomainInput!) { batchSetDomain(input: $input) }", "batchSetDomain",
        _resources(p.entity), lambda chunk: {"domainUrn": None, "resources": chunk},
    )


@step("add_to_data_product", label="Add to data product", description="Add an entity to a data product.", group="Metadata", params=DataProductParams, bulk_param="entity")
def add_to_data_product(p: DataProductParams, ctx: RunContext) -> dict:
    return _batched(
        ctx, "mutation($input: BatchSetDataProductInput!) { batchSetDataProduct(input: $input) }", "batchSetDataProduct",
        _urns(p.entity), lambda chunk: {"dataProductUrn": p.dataProduct, "resourceUrns": chunk},
    )


@step("set_structured_property", label="Set structured property", description="Set a structured property value on an entity.", group="Metadata", params=StructuredPropertyParams, bulk_param="entity")
def set_structured_property(p: StructuredPropertyParams, ctx: RunContext) -> dict:
    values = []
    for raw in _urns(p.values):
        try:
            values.append({"numberValue": float(raw)})
        except ValueError:
            values.append({"stringValue": raw})
    results = [
        ctx.graphql(
            "mutation($input: UpsertStructuredPropertiesInput!) { upsertStructuredProperties(input: $input) { properties { structuredProperty { urn } } } }",
            {"input": {"assetUrn": urn, "structuredPropertyInputParams": [{"structuredPropertyUrn": p.property, "values": values}]}},
            mutation="upsertStructuredProperties",
        )
        for urn in _urns(p.entity)
    ]
    return {"results": results}


@step("deprecate", label="Deprecate", description="Mark an entity as deprecated.", group="Metadata", params=DeprecateParams, bulk_param="entity")
def deprecate(p: DeprecateParams, ctx: RunContext) -> dict:
    extra = {}
    if p.note:
        extra["note"] = p.note
    if p.replacement:
        extra["replacement"] = p.replacement
    if p.decommissionTime is not None:
        extra["decommissionTime"] = p.decommissionTime
    return _batched(
        ctx, "mutation($input: BatchUpdateDeprecationInput!) { batchUpdateDeprecation(input: $input) }", "batchUpdateDeprecation",
        _resources(p.entity), lambda chunk: {"deprecated": True, "resources": chunk, **extra},
    )


@step("undeprecate", label="Remove deprecation", description="Clear the deprecation flag.", group="Metadata", params=EntityParams, bulk_param="entity")
def undeprecate(p: EntityParams, ctx: RunContext) -> dict:
    return _batched(
        ctx, "mutation($input: BatchUpdateDeprecationInput!) { batchUpdateDeprecation(input: $input) }", "batchUpdateDeprecation",
        _resources(p.entity), lambda chunk: {"deprecated": False, "resources": chunk},
    )


@step("update_description", label="Update description", description="Replace the entity description.", group="Metadata", params=DescriptionParams, bulk_param="entity")
def update_description(p: DescriptionParams, ctx: RunContext) -> dict:
    return {
        "results": [
            ctx.graphql(
                "mutation($input: DescriptionUpdateInput!) { updateDescription(input: $input) }",
                {"input": {"resourceUrn": urn, "description": p.description}},
                mutation="updateDescription",
            )
            for urn in _urns(p.entity)
        ]
    }
