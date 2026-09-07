"""Lookup steps — parameter validation, pagination edges, helpers."""

import pytest

from datahub_workflow_actions.steps import RunContext, get_step
from datahub_workflow_actions.steps.lookup import MAX_RESULTS_CAP, PAGE, _entity_row, _filters_from, _list, _pluck


def run(type_, params, ctx):
    d = get_step(type_)
    return d.run(d.params.model_validate(params), ctx)


class ScriptedGraph:
    """Returns the given pages in order (one per call) and records inputs."""

    def __init__(self, pages):
        self.pages, self.calls = list(pages), []

    def execute_graphql(self, query, variables=None, **_):
        self.calls.append(variables)
        return self.pages.pop(0) if self.pages else {}


def page(op, urns, next_id=None, total=None, **extra):
    results = [{"entity": {"urn": u, "type": "DATASET", "properties": {"name": u.rsplit(":", 1)[1]}}, **extra} for u in urns]
    body = {"total": total if total is not None else len(urns), "searchResults": results}
    if op != "listDataProductAssets":
        body["nextScrollId"] = next_id
    return {op: body}


# ------------------------------------------------------------------ helpers ---


def test_list_accepts_json_arrays_comma_strings_and_lists():
    assert _list('["DATASET", "CHART"]') == ["DATASET", "CHART"]
    assert _list("DATASET, CHART,,") == ["DATASET", "CHART"]
    assert _list(["a", "", " ", "b"]) == ["a", "b"]
    assert _list(None) == [] and _list("") == []


def test_entity_row_name_fallbacks():
    assert _entity_row({"urn": "urn:li:corpuser:jdoe", "type": "CORP_USER", "username": "jdoe"})["name"] == "jdoe"
    assert _entity_row({"urn": "urn:li:x:1", "type": "X"})["name"] == "urn:li:x:1"
    assert _entity_row({"urn": "urn:li:x:1", "properties": {"name": "Nice"}})["name"] == "Nice"
    assert _entity_row(None) is None and _entity_row({"type": "X"}) is None


def test_pluck_paths():
    data = {"dataset": {"schemaMetadata": {"fields": [{"fieldPath": "id"}, {"fieldPath": "name"}]}}, "list": [1, 2]}
    assert _pluck(data, None) is data and _pluck(data, "") is data
    assert _pluck(data, "dataset.schemaMetadata.fields.fieldPath") == ["id", "name"]
    assert _pluck(data, "dataset.schemaMetadata.fields[1].fieldPath") == "name"
    assert _pluck(data, "list[5]") is None and _pluck(data, "missing.deep") is None
    assert _pluck(data, "list.foo") == [None, None]
    assert _pluck(3, "a") is None


def test_filters_from_variants():
    assert _filters_from({}, None) == []
    assert _filters_from({"owner": ["urn:li:corpuser:a", "urn:li:corpGroup:g"]}, None) == [
        {"and": [{"field": "owners", "values": ["urn:li:corpuser:a", "urn:li:corpGroup:g"], "condition": "EQUAL"}]}
    ]
    single = _filters_from({}, {"field": "origin", "values": "PROD, DEV", "condition": "IN"})
    assert single == [{"and": [{"field": "origin", "values": ["PROD", "DEV"], "condition": "IN"}]}]
    with pytest.raises(ValueError):
        _filters_from({}, [{"values": ["x"]}])
    with pytest.raises(ValueError):
        _filters_from({}, "{not json")


# ------------------------------------------------------------------- search ---


def test_search_omits_empty_types_and_filters_and_uses_page_size():
    graph = ScriptedGraph([page("scrollAcrossEntities", ["urn:li:dataset:1"])])
    out = run("search", {"types": "", "query": ""}, RunContext(graph=graph))
    inp = graph.calls[0]["input"]
    assert "types" not in inp and "orFilters" not in inp and inp["query"] == "*" and inp["count"] == PAGE
    assert out["urns"] == ["urn:li:dataset:1"] and out["total"] == 1


def test_search_stops_on_missing_scroll_id_and_caps_results():
    graph = ScriptedGraph([
        page("scrollAcrossEntities", ["urn:li:dataset:1", "urn:li:dataset:2"], next_id="2", total=10),
        page("scrollAcrossEntities", ["urn:li:dataset:3", "urn:li:dataset:4"], next_id="4", total=10),
    ])
    out = run("search", {"maxResults": 3}, RunContext(graph=graph))
    assert out["urns"] == ["urn:li:dataset:1", "urn:li:dataset:2", "urn:li:dataset:3"] and out["total"] == 10
    assert graph.calls[1]["input"]["count"] == 1 and graph.calls[1]["input"]["scrollId"] == "2"
    graph = ScriptedGraph([page("scrollAcrossEntities", ["urn:li:dataset:1"], next_id=None, total=99)])
    assert run("search", {}, RunContext(graph=graph))["urns"] == ["urn:li:dataset:1"]  # no next id → done


def test_search_empty_page_ends_pagination():
    graph = ScriptedGraph([page("scrollAcrossEntities", [], next_id="x", total=0)])
    out = run("search", {}, RunContext(graph=graph))
    assert out["urns"] == [] and len(graph.calls) == 1


@pytest.mark.parametrize("params", [{"maxResults": 0}, {"maxResults": MAX_RESULTS_CAP + 1}, {"bogus": 1}])
def test_search_param_validation(params):
    with pytest.raises(ValueError):
        get_step("search").params.model_validate(params)


def test_search_partial_results_survive_a_vanished_graph():
    class Flaky(ScriptedGraph):
        def execute_graphql(self, query, variables=None, **_):
            result = super().execute_graphql(query, variables)
            return result or {"scrollAcrossEntities": None}

    graph = Flaky([page("scrollAcrossEntities", ["urn:li:dataset:1"], next_id="1", total=5)])
    out = run("search", {}, RunContext(graph=graph))
    assert out["urns"] == ["urn:li:dataset:1"] and out["total"] == 5


# --------------------------------------------------------- data product ---


def test_data_product_assets_stops_at_total_and_passes_types():
    graph = ScriptedGraph([page("listDataProductAssets", ["urn:li:dataset:1", "urn:li:dataset:2"], total=2)])
    out = run("data_product_assets", {"dataProduct": "urn:li:dataProduct:p", "types": "DATASET"}, RunContext(graph=graph))
    assert out["urns"] == ["urn:li:dataset:1", "urn:li:dataset:2"] and len(graph.calls) == 1
    assert graph.calls[0]["input"]["types"] == ["DATASET"] and graph.calls[0]["urn"] == "urn:li:dataProduct:p"


def test_data_product_assets_empty_and_offline():
    graph = ScriptedGraph([page("listDataProductAssets", [], total=0)])
    assert run("data_product_assets", {"dataProduct": "urn:li:dataProduct:p"}, RunContext(graph=graph))["urns"] == []
    out = run("data_product_assets", {"dataProduct": "urn:li:dataProduct:p"}, RunContext(graph=None))
    assert out["dryRun"] and out["urns"] == []
    with pytest.raises(ValueError):
        get_step("data_product_assets").params.model_validate({})


# ------------------------------------------------------------------ lineage ---


def test_lineage_validation_and_offline():
    params = get_step("lineage").params
    assert params.model_validate({"entity": "urn:li:dataset:x", "direction": " upstream "}).direction == "UPSTREAM"
    for bad in [{"entity": "u", "direction": "SIDEWAYS"}, {"entity": "u", "hops": 0}, {"entity": "u", "hops": 11}, {"direction": "UPSTREAM"}]:
        with pytest.raises(ValueError):
            params.model_validate(bad)
    out = run("lineage", {"entity": "urn:li:dataset:x"}, RunContext(graph=None))
    assert out["dryRun"] and out["degrees"] == {}


def test_lineage_pages_and_passes_types_and_hops():
    graph = ScriptedGraph([
        page("scrollAcrossLineage", ["urn:li:dataset:1"], next_id="1", total=2, degree=1),
        page("scrollAcrossLineage", ["urn:li:dataset:2"], next_id=None, total=2, degree=3),
    ])
    out = run("lineage", {"entity": "urn:li:dataset:x", "hops": 3, "types": ["DATASET"]}, RunContext(graph=graph))
    inp = graph.calls[0]["input"]
    assert inp["types"] == ["DATASET"] and inp["orFilters"][0]["and"][0]["values"] == ["1", "2", "3+"]
    assert out["degrees"] == {"urn:li:dataset:1": 1, "urn:li:dataset:2": 3} and out["total"] == 2


# ------------------------------------------------------------------ graphql ---


def test_graphql_variables_validation_and_defaults():
    graph = ScriptedGraph([{"me": {"corpUser": {"urn": "urn:li:corpuser:x"}}}])
    out = run("graphql", {"query": "query { me { corpUser { urn } } }"}, RunContext(graph=graph))
    assert out["value"] == out["data"] and graph.calls[0] == {}
    with pytest.raises(ValueError):
        run("graphql", {"query": "query { me }", "variables": "{oops"}, RunContext(graph=graph))
    with pytest.raises(ValueError):
        run("graphql", {"query": "query { me }", "variables": "[1, 2]"}, RunContext(graph=graph))
    with pytest.raises(ValueError):
        get_step("graphql").params.model_validate({"query": "  MUTATION { x }"})
    out = run("graphql", {"query": "query { me }"}, RunContext(graph=None))
    assert out["dryRun"] and out["value"] is None


def test_run_context_query_helper():
    graph = ScriptedGraph([{"scrollAcrossEntities": {"total": 1}}])
    ctx = RunContext(graph=graph, dry_run=True)
    assert ctx.query("query { scrollAcrossEntities }", {}, operation="scrollAcrossEntities") == {"total": 1}
    assert RunContext(graph=None).query("query { x }", {}, operation="x") is None


def test_lineage_degree_values_match_the_gms_filter_vocabulary():
    from datahub_workflow_actions.steps.lookup import degree_values

    assert degree_values(1) == ["1"]
    assert degree_values(2) == ["1", "2"]
    assert degree_values(3) == ["1", "2", "3+"]
    assert degree_values(10) == ["1", "2", "3+"]  # "3+" already means everything beyond two hops
