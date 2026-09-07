"""Metadata steps — every step accepts a list and uses GMS batch mutations, chunked."""

import pytest

from datahub_workflow_actions.steps import RunContext, get_step
from datahub_workflow_actions.steps.metadata import BATCH_CHUNK, split_list
from tests.conftest import DATASET, FakeGraph

TWO = [DATASET, "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.sales.customers,PROD)"]
RES = [{"resourceUrn": u} for u in TWO]


def run(type_, params, graph=None):
    d = get_step(type_)
    return d.run(d.params.model_validate(params), RunContext(graph=graph))


def only_call(graph):
    assert len(graph.calls) == 1
    query, variables = graph.calls[0]
    return query.split("{", 1)[1].split("(", 1)[0].strip(), variables["input"]


@pytest.mark.parametrize(
    "type_, params, mutation, expected",
    [
        ("add_tag", {"tag": "urn:li:tag:a"}, "batchAddTags", {"tagUrns": ["urn:li:tag:a"], "resources": RES}),
        ("remove_tag", {"tag": ["urn:li:tag:a", "urn:li:tag:b"]}, "batchRemoveTags", {"tagUrns": ["urn:li:tag:a", "urn:li:tag:b"], "resources": RES}),
        ("add_term", {"term": "urn:li:glossaryTerm:t"}, "batchAddTerms", {"termUrns": ["urn:li:glossaryTerm:t"], "resources": RES}),
        ("remove_term", {"term": "urn:li:glossaryTerm:t"}, "batchRemoveTerms", {"termUrns": ["urn:li:glossaryTerm:t"], "resources": RES}),
        ("set_domain", {"domain": "urn:li:domain:d"}, "batchSetDomain", {"domainUrn": "urn:li:domain:d", "resources": RES}),
        ("clear_domain", {}, "batchSetDomain", {"domainUrn": None, "resources": RES}),
        ("add_to_data_product", {"dataProduct": "urn:li:dataProduct:p"}, "batchSetDataProduct", {"dataProductUrn": "urn:li:dataProduct:p", "resourceUrns": TWO}),
        ("deprecate", {"note": "old", "decommissionTime": 1700000000000}, "batchUpdateDeprecation", {"deprecated": True, "resources": RES, "note": "old", "decommissionTime": 1700000000000}),
        ("undeprecate", {}, "batchUpdateDeprecation", {"deprecated": False, "resources": RES}),
        ("remove_owner", {"owner": "urn:li:corpuser:u", "ownershipType": "urn:li:ownershipType:custom"}, "batchRemoveOwners", {"ownerUrns": ["urn:li:corpuser:u"], "resources": RES, "ownershipTypeUrn": "urn:li:ownershipType:custom"}),
    ],
)
def test_batch_mutation_shapes(type_, params, mutation, expected):
    graph = FakeGraph()
    out = run(type_, {"entity": TWO, **params}, graph)
    assert only_call(graph) == (mutation, expected)
    assert out == {"mutation": mutation, "result": True}


def test_add_owner_builds_owner_inputs_per_owner():
    graph = FakeGraph()
    run("add_owner", {"entity": TWO, "owner": "urn:li:corpuser:u, urn:li:corpGroup:g", "ownershipType": "DATA_STEWARD"}, graph)
    mutation, inp = only_call(graph)
    assert mutation == "batchAddOwners" and inp["resources"] == RES
    assert inp["owners"] == [
        {"ownerUrn": "urn:li:corpuser:u", "ownerEntityType": "CORP_USER", "type": "DATA_STEWARD"},
        {"ownerUrn": "urn:li:corpGroup:g", "ownerEntityType": "CORP_GROUP", "type": "DATA_STEWARD"},
    ]


def test_steps_without_a_batch_api_loop_per_entity_inside_one_call():
    graph = FakeGraph()
    out = run("update_description", {"entity": TWO, "description": "d"}, graph)
    assert [v["input"]["resourceUrn"] for _, v in graph.calls] == TWO and len(out["results"]) == 2
    graph = FakeGraph()
    out = run("set_structured_property", {"entity": TWO, "property": "urn:li:structuredProperty:sp", "values": ["1", "x"]}, graph)
    assert len(graph.calls) == 2 and len(out["results"]) == 2
    assert graph.calls[0][1]["input"]["structuredPropertyInputParams"][0]["values"] == [{"numberValue": 1.0}, {"stringValue": "x"}]


@pytest.mark.parametrize("type_, params, list_key", [
    ("set_domain", {"domain": "urn:li:domain:d"}, "resources"),
    ("add_to_data_product", {"dataProduct": "urn:li:dataProduct:p"}, "resourceUrns"),
    ("undeprecate", {}, "resources"),
    ("remove_term", {"term": "urn:li:glossaryTerm:t"}, "resources"),
])
def test_every_batch_mutation_chunks_large_lists(type_, params, list_key):
    graph = FakeGraph()
    urns = [f"urn:li:dataset:{i}" for i in range(2 * BATCH_CHUNK + 1)]
    out = run(type_, {"entity": urns, **params}, graph)
    assert out["chunks"] == 3 and [len(v["input"][list_key]) for _, v in graph.calls] == [BATCH_CHUNK, BATCH_CHUNK, 1]


def test_comma_separated_entities_split_outside_parentheses():
    joined = ", ".join(TWO)
    assert split_list(joined) == TWO
    graph = FakeGraph()
    run("add_tag", {"entity": joined, "tag": "urn:li:tag:a"}, graph)
    assert only_call(graph)[1]["resources"] == RES


def test_dry_run_describes_a_single_batch_without_a_graph():
    out = run("clear_domain", {"entity": TWO})
    assert out["dryRun"] and out["mutation"] == "batchSetDomain" and out["variables"]["input"]["resources"] == RES


def test_empty_entity_list_makes_one_empty_call():
    graph = FakeGraph()
    run("add_tag", {"entity": [], "tag": "urn:li:tag:a"}, graph)
    assert only_call(graph)[1]["resources"] == []
