"""Metadata steps — DataHub GraphQL mutations, run as the executor's actor."""

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


@step("add_tag", label="Add tag", description="Attach a tag to an entity.", group="Metadata", params=TagParams)
def add_tag(p: TagParams, ctx: RunContext) -> dict:
    return ctx.graphql(
        "mutation($input: BatchAddTagsInput!) { batchAddTags(input: $input) }",
        {"input": {"tagUrns": _urns(p.tag), "resources": _resources(p.entity)}},
        mutation="batchAddTags",
    )


@step("remove_tag", label="Remove tag", description="Remove a tag from an entity.", group="Metadata", params=TagParams)
def remove_tag(p: TagParams, ctx: RunContext) -> dict:
    return ctx.graphql(
        "mutation($input: BatchRemoveTagsInput!) { batchRemoveTags(input: $input) }",
        {"input": {"tagUrns": _urns(p.tag), "resources": _resources(p.entity)}},
        mutation="batchRemoveTags",
    )


@step("add_term", label="Add glossary term", description="Attach a glossary term to an entity.", group="Metadata", params=TermParams)
def add_term(p: TermParams, ctx: RunContext) -> dict:
    return ctx.graphql(
        "mutation($input: BatchAddTermsInput!) { batchAddTerms(input: $input) }",
        {"input": {"termUrns": _urns(p.term), "resources": _resources(p.entity)}},
        mutation="batchAddTerms",
    )


@step("remove_term", label="Remove glossary term", description="Remove a glossary term from an entity.", group="Metadata", params=TermParams)
def remove_term(p: TermParams, ctx: RunContext) -> dict:
    return ctx.graphql(
        "mutation($input: BatchRemoveTermsInput!) { batchRemoveTerms(input: $input) }",
        {"input": {"termUrns": _urns(p.term), "resources": _resources(p.entity)}},
        mutation="batchRemoveTerms",
    )


@step("add_owner", label="Add owner", description="Add a user or group as an owner.", group="Metadata", params=OwnerParams)
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
    return ctx.graphql(
        "mutation($input: BatchAddOwnersInput!) { batchAddOwners(input: $input) }",
        {"input": {"owners": owners, "resources": _resources(p.entity)}},
        mutation="batchAddOwners",
    )


@step("remove_owner", label="Remove owner", description="Remove a user or group from the owners.", group="Metadata", params=RemoveOwnerParams)
def remove_owner(p: RemoveOwnerParams, ctx: RunContext) -> dict:
    payload = {"ownerUrns": _urns(p.owner), "resources": _resources(p.entity)}
    if p.ownershipType:
        payload["ownershipTypeUrn"] = p.ownershipType
    return ctx.graphql(
        "mutation($input: BatchRemoveOwnersInput!) { batchRemoveOwners(input: $input) }",
        {"input": payload},
        mutation="batchRemoveOwners",
    )


@step("set_domain", label="Set domain", description="Move an entity into a domain.", group="Metadata", params=DomainParams)
def set_domain(p: DomainParams, ctx: RunContext) -> dict:
    return ctx.graphql(
        "mutation($input: BatchSetDomainInput!) { batchSetDomain(input: $input) }",
        {"input": {"domainUrn": p.domain, "resources": _resources(p.entity)}},
        mutation="batchSetDomain",
    )


@step("clear_domain", label="Clear domain", description="Remove an entity from its domain.", group="Metadata", params=EntityParams)
def clear_domain(p: EntityParams, ctx: RunContext) -> dict:
    results = [
        ctx.graphql("mutation($urn: String!) { unsetDomain(entityUrn: $urn) }", {"urn": urn}, mutation="unsetDomain")
        for urn in _urns(p.entity)
    ]
    return {"results": results}


@step("add_to_data_product", label="Add to data product", description="Add an entity to a data product.", group="Metadata", params=DataProductParams)
def add_to_data_product(p: DataProductParams, ctx: RunContext) -> dict:
    return ctx.graphql(
        "mutation($input: BatchSetDataProductInput!) { batchSetDataProduct(input: $input) }",
        {"input": {"dataProductUrn": p.dataProduct, "resourceUrns": _urns(p.entity)}},
        mutation="batchSetDataProduct",
    )


@step("set_structured_property", label="Set structured property", description="Set a structured property value on an entity.", group="Metadata", params=StructuredPropertyParams)
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


@step("deprecate", label="Deprecate", description="Mark an entity as deprecated.", group="Metadata", params=DeprecateParams)
def deprecate(p: DeprecateParams, ctx: RunContext) -> dict:
    results = []
    for urn in _urns(p.entity):
        payload = {"urn": urn, "deprecated": True}
        if p.note:
            payload["note"] = p.note
        if p.replacement:
            payload["replacement"] = p.replacement
        if p.decommissionTime is not None:
            payload["decommissionTime"] = p.decommissionTime
        results.append(
            ctx.graphql(
                "mutation($input: UpdateDeprecationInput!) { updateDeprecation(input: $input) }",
                {"input": payload},
                mutation="updateDeprecation",
            )
        )
    return {"results": results}


@step("undeprecate", label="Remove deprecation", description="Clear the deprecation flag.", group="Metadata", params=EntityParams)
def undeprecate(p: EntityParams, ctx: RunContext) -> dict:
    return {
        "results": [
            ctx.graphql(
                "mutation($input: UpdateDeprecationInput!) { updateDeprecation(input: $input) }",
                {"input": {"urn": urn, "deprecated": False}},
                mutation="updateDeprecation",
            )
            for urn in _urns(p.entity)
        ]
    }


@step("update_description", label="Update description", description="Replace the entity description.", group="Metadata", params=DescriptionParams)
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
