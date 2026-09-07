"""Lookup steps (§19): pagination, filters, dry-run behaviour, GraphQL plucking."""

from datahub_workflow_actions.steps import RunContext, get_step
from datahub_workflow_actions.steps.lookup import _filters_from, _pluck


def ent(i, type_="DATASET"):
    return {"urn": f"urn:li:dataset:{i}", "type": type_, "properties": {"name": f"table_{i}"}}


class PagedGraph:
    """Serves scroll / offset pages of `n` entities, `page` at a time."""

    def __init__(self, n, page=2):
        self.n, self.page, self.calls = n, page, []

    def execute_graphql(self, query, variables=None, **_):
        self.calls.append(variables)
        inp = variables.get("input") or {}
        if "scrollAcrossEntities" in query or "scrollAcrossLineage" in query:
            start = int(inp.get("scrollId") or 0)
            op = "scrollAcrossEntities" if "scrollAcrossEntities" in query else "scrollAcrossLineage"
            end = min(start + min(self.page, inp["count"]), self.n)
            results = [{"entity": ent(i), "degree": 1} for i in range(start, end)]
            return {op: {"total": self.n, "nextScrollId": str(end) if end < self.n else None, "searchResults": results}}
        if "listDataProductAssets" in query:
            start = inp["start"]
            end = min(start + min(self.page, inp["count"]), self.n)
            return {"listDataProductAssets": {"start": start, "count": end - start, "total": self.n, "searchResults": [{"entity": ent(i)} for i in range(start, end)]}}
        return {"dataset": {"schemaMetadata": {"fields": [{"fieldPath": "id"}, {"fieldPath": "name"}]}}}


def run(type_, params, ctx):
    d = get_step(type_)
    return d.run(d.params.model_validate(params), ctx)


def test_search_paginates_and_maps_entities():
    graph = PagedGraph(5, page=2)
    out = run("search", {"types": "DATASET", "query": "orders", "domain": "urn:li:domain:sales", "maxResults": 100}, RunContext(graph=graph))
    assert out["total"] == 5 and len(out["urns"]) == 5
    assert out["entities"][0] == {"urn": "urn:li:dataset:0", "type": "DATASET", "name": "table_0"}
    assert len(graph.calls) == 3
    first = graph.calls[0]["input"]
    assert first["types"] == ["DATASET"] and first["query"] == "orders"
    assert first["orFilters"] == [{"and": [{"field": "domains", "values": ["urn:li:domain:sales"], "condition": "EQUAL"}]}]


def test_search_respects_max_results():
    graph = PagedGraph(10, page=4)
    out = run("search", {"maxResults": 5}, RunContext(graph=graph))
    assert len(out["urns"]) == 5 and out["total"] == 10


def test_search_runs_in_dry_run_but_degrades_without_graph():
    assert run("search", {}, RunContext(graph=PagedGraph(1), dry_run=True))["urns"] == ["urn:li:dataset:0"]
    out = run("search", {}, RunContext(graph=None, dry_run=True))
    assert out["dryRun"] and out["urns"] == [] and out["total"] == 0


def test_filters_merge_simple_and_raw():
    ors = _filters_from({"tag": "urn:li:tag:pii", "domain": None}, '[{"field": "origin", "values": ["PROD"], "negated": true}]')
    assert ors == [{"and": [
        {"field": "tags", "values": ["urn:li:tag:pii"], "condition": "EQUAL"},
        {"field": "origin", "values": ["PROD"], "condition": "EQUAL", "negated": True},
    ]}]
    # a full orFilters list keeps its branches, each gaining the simple conditions
    ors = _filters_from({"platform": "urn:li:dataPlatform:snowflake"}, [{"and": [{"field": "a", "values": ["1"]}]}, {"and": [{"field": "b", "values": ["2"]}]}])
    assert len(ors) == 2 and ors[1]["and"][0]["field"] == "platform" and ors[1]["and"][1]["field"] == "b"


def test_data_product_assets_uses_offset_pagination():
    graph = PagedGraph(3, page=2)
    out = run("data_product_assets", {"dataProduct": "urn:li:dataProduct:dp", "types": ["DATASET", "DASHBOARD"]}, RunContext(graph=graph))
    assert out["urns"] == ["urn:li:dataset:0", "urn:li:dataset:1", "urn:li:dataset:2"]
    assert graph.calls[0]["urn"] == "urn:li:dataProduct:dp" and graph.calls[0]["input"]["types"] == ["DATASET", "DASHBOARD"]
    assert graph.calls[1]["input"]["start"] == 2


def test_lineage_filters_by_degree_and_reports_degrees():
    graph = PagedGraph(2, page=5)
    out = run("lineage", {"entity": "urn:li:dataset:root", "direction": "upstream", "hops": 2}, RunContext(graph=graph))
    inp = graph.calls[0]["input"]
    assert inp["direction"] == "UPSTREAM" and inp["urn"] == "urn:li:dataset:root"
    assert inp["orFilters"] == [{"and": [{"field": "degree", "values": ["1", "2"], "condition": "EQUAL"}]}]
    assert out["degrees"] == {"urn:li:dataset:0": 1, "urn:li:dataset:1": 1}


def test_graphql_lookup_plucks_and_refuses_mutations():
    graph = PagedGraph(0)
    out = run("graphql", {"query": "query($urn: String!) { dataset(urn: $urn) { schemaMetadata { fields { fieldPath } } } }", "variables": '{"urn": "urn:li:dataset:x"}', "path": "dataset.schemaMetadata.fields.fieldPath"}, RunContext(graph=graph))
    assert out["value"] == ["id", "name"]
    assert graph.calls[0] == {"urn": "urn:li:dataset:x"}
    d = get_step("graphql")
    try:
        d.params.model_validate({"query": "mutation { x }"})
        assert False, "mutations must be rejected"
    except ValueError:
        pass
    assert _pluck({"a": [{"b": 1}, {"b": 2}]}, "a[1].b") == 2


def test_catalog_lists_lookups_with_outputs_and_bulk_params():
    from datahub_workflow_actions.steps import catalog

    by_type = {c["type"]: c for c in catalog()}
    assert set(by_type["search"]["outputs"]) == {"urns", "entities", "total"}
    assert by_type["search"]["group"] == "Lookup" and by_type["search"]["bulkParam"] is None
    assert by_type["add_tag"]["bulkParam"] == "entity"
