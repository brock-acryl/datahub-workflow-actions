"""§19 — forEach loops, per-item conditions and automatic batching, end to end
through the engine (lookup → fan-out → bulk mutations)."""

import json

import pytest

from datahub_workflow_actions.contract import Rule, RulesConfig
from datahub_workflow_actions.engine import Engine
from datahub_workflow_actions.state import InMemoryStateStore
from datahub_workflow_actions.steps import RunContext, StepParams, catalog, get_step, step
from tests.conftest import DATASET, WF, FakeGraph

# ---------------------------------------------------------------- fixtures ---


class RecordParams(StepParams):
    value: str


@step("_loop_record", label="record", description="test helper", group="Test", params=RecordParams)
def _loop_record(p: RecordParams, ctx: RunContext):
    return {"value": p.value, "index": ctx.context.get("index")}


class LookupGraph(FakeGraph):
    """Serves data-product / scroll pages of `n` datasets and accepts every mutation."""

    def __init__(self, n, page=100, types=None, fail_times=0):
        super().__init__(fail_times=fail_times)
        self.n, self.page = n, page
        self.types = types or (lambda i: "DATASET")

    def entity(self, i):
        return {"urn": f"urn:li:dataset:{i}", "type": self.types(i), "properties": {"name": f"t{i}"}}

    def execute_graphql(self, query, variables=None, **_):
        if query.lstrip().startswith("mutation"):
            return super().execute_graphql(query, variables)
        self.calls.append((query, variables))
        inp = (variables or {}).get("input") or {}
        if "listDataProductAssets" in query:
            start = inp["start"]
            end = min(start + min(self.page, inp["count"]), self.n)
            return {"listDataProductAssets": {"start": start, "count": end - start, "total": self.n, "searchResults": [{"entity": self.entity(i)} for i in range(start, end)]}}
        if "scrollAcrossEntities" in query:
            start = int(inp.get("scrollId") or 0)
            end = min(start + min(self.page, inp["count"]), self.n)
            return {"scrollAcrossEntities": {"total": self.n, "nextScrollId": str(end) if end < self.n else None, "searchResults": [{"entity": self.entity(i)} for i in range(start, end)]}}
        if "scrollAcrossLineage" in query:
            return {"scrollAcrossLineage": {"total": 1, "nextScrollId": None, "searchResults": [{"degree": 1, "entity": self.entity(999)}]}}
        return {}


def mutation_calls(graph):
    return [(q.split("{", 1)[1].split("(", 1)[0].strip(), v["input"]) for q, v in graph.calls if q.lstrip().startswith("mutation")]


def query_calls(graph):
    return [q for q, _ in graph.calls if q.lstrip().startswith("query")]


def rule(steps, **overrides):
    body = {"id": "r", "workflowUrn": WF, "on": {"operation": "COMPLETED", "result": "ACCEPTED"}, "steps": steps}
    body.update(overrides)
    return Rule.model_validate(body)


def ctx(**extra):
    base = {
        "event": {"id": "evt-1", "operation": "COMPLETED", "result": "ACCEPTED"},
        "entity": {"urn": DATASET, "type": "DATASET", "owners": ["urn:li:corpuser:a", "urn:li:corpGroup:g"], "tags": []},
        "requester": {"urn": "urn:li:corpuser:jdoe"},
    }
    base.update(extra)
    return base


def engine(graph, dry_run=False, state=None):
    return Engine(RunContext(graph=graph), dry_run=dry_run, state=state or InMemoryStateStore())


TAG_EACH = {"id": "tag", "type": "add_tag", "forEach": "{{ lookup.urns }}", "params": {"entity": "{{ item }}", "tag": "urn:li:tag:t"}}


# ------------------------------------------------------------ list sources ---


def test_empty_none_and_scalar_for_each_lists(fake_graph):
    run = engine(fake_graph).run_rule(rule([TAG_EACH]), ctx(lookup={"urns": []}))
    assert run.status == "ok" and run.steps[0].reason == "0 item(s)" and run.steps[0].items == [] and not fake_graph.calls

    run = engine(fake_graph).run_rule(rule([TAG_EACH]), ctx(lookup={"urns": None}))
    assert run.steps[0].status == "ok" and run.steps[0].reason == "0 item(s)"

    run = engine(fake_graph).run_rule(rule([TAG_EACH]), ctx(lookup={"urns": "urn:li:dataset:solo"}))
    assert run.steps[0].reason == "1 item(s)"  # a lone scalar is a one-element list, run per item
    assert mutation_calls(fake_graph)[0][1]["resources"] == [{"resourceUrn": "urn:li:dataset:solo"}]


def test_for_each_template_errors_fail_the_step_before_any_call(fake_graph):
    run = engine(fake_graph).run_rule(rule([TAG_EACH]), ctx())  # no `lookup` in context
    assert run.steps[0].status == "failed" and run.steps[0].error.startswith("forEach:")
    assert run.status == "failed" and not fake_graph.calls


def test_dotted_path_for_each_over_context_list(fake_graph):
    owner_each = {"id": "own", "type": "add_owner", "forEach": "entity.owners", "params": {"entity": "{{ entity.urn }}", "owner": "{{ item }}"}}
    run = engine(fake_graph).run_rule(rule([owner_each]), ctx())
    # each owner renders a different `owner` param → nothing to merge → 2 calls
    assert run.steps[0].reason == "2 item(s), 2 call(s)"
    owners = [c[1]["owners"][0]["ownerUrn"] for c in mutation_calls(fake_graph)]
    assert owners == ["urn:li:corpuser:a", "urn:li:corpGroup:g"]


# --------------------------------------------------------------- batching ---


def test_auto_batching_merges_identical_settings_and_chunks(fake_graph):
    items = [f"urn:li:dataset:{i}" for i in range(7)]
    run = engine(fake_graph).run_rule(rule([{**TAG_EACH, "batch": {"size": 3}}]), ctx(lookup={"urns": items}))
    step = run.steps[0]
    assert step.status == "ok" and step.reason == "7 item(s), 3 call(s)"
    sent = [[r["resourceUrn"] for r in c[1]["resources"]] for c in mutation_calls(fake_graph)]
    assert sent == [items[0:3], items[3:6], items[6:7]]
    assert [r.reason for r in step.items] == ["batch 1: 3 item(s)", "batch 2: 3 item(s)", "batch 3: 1 item(s)"]
    assert all(r.idempotencyKey.endswith(f"|batch{n}") for n, r in enumerate(step.items, 1))
    # the step's output is the list of per-call outputs, in call order
    assert [o["mutation"] for o in step.output] == ["batchAddTags"] * 3


def test_bulk_values_that_render_to_lists_are_flattened(fake_graph):
    groups = [{"urns": ["urn:li:dataset:1", "urn:li:dataset:2"]}, {"urns": ["urn:li:dataset:3"]}, {"urns": []}]
    each = {**TAG_EACH, "params": {"entity": "{{ item.urns }}", "tag": "urn:li:tag:t"}}
    run = engine(fake_graph).run_rule(rule([each]), ctx(lookup={"urns": groups}))
    assert run.steps[0].reason == "3 item(s), 1 call(s)"
    assert [r["resourceUrn"] for r in mutation_calls(fake_graph)[0][1]["resources"]] == ["urn:li:dataset:1", "urn:li:dataset:2", "urn:li:dataset:3"]


def test_batch_size_one_and_duplicates_are_preserved(fake_graph):
    items = ["urn:li:dataset:a", "urn:li:dataset:a", "urn:li:dataset:b"]
    run = engine(fake_graph).run_rule(rule([{**TAG_EACH, "batch": {"size": 1}}]), ctx(lookup={"urns": items}))
    assert run.steps[0].reason == "3 item(s), 3 call(s)"
    assert [c[1]["resources"][0]["resourceUrn"] for c in mutation_calls(fake_graph)] == items


def test_items_mode_forces_one_call_per_item_with_item_reasons(fake_graph):
    items = [f"urn:li:dataset:{i}" for i in range(3)]
    run = engine(fake_graph).run_rule(rule([{**TAG_EACH, "batch": {"mode": "items"}}]), ctx(lookup={"urns": items}))
    step = run.steps[0]
    assert step.reason == "3 item(s)" and [r.reason for r in step.items] == ["item 0", "item 1", "item 2"]
    assert len(mutation_calls(fake_graph)) == 3
    assert [r.idempotencyKey.rsplit("|", 1)[1] for r in step.items] == ["0", "1", "2"]


def test_steps_without_a_bulk_param_run_per_item_even_in_auto(fake_graph):
    each = {"id": "rec", "type": "_loop_record", "forEach": "{{ lookup.urns }}", "params": {"value": "{{ item }}"}}
    run = engine(fake_graph).run_rule(rule([each]), ctx(lookup={"urns": ["x", "y"]}))
    assert run.steps[0].reason == "2 item(s)"
    assert [o["value"] for o in run.steps[0].output] == ["x", "y"] and [o["index"] for o in run.steps[0].output] == [0, 1]


def test_mixed_settings_group_by_rendered_params_in_first_seen_order(fake_graph):
    items = [
        {"urn": "urn:li:dataset:1", "tag": "urn:li:tag:x"},
        {"urn": "urn:li:dataset:2", "tag": "urn:li:tag:y"},
        {"urn": "urn:li:dataset:3", "tag": "urn:li:tag:x"},
        {"urn": "urn:li:dataset:4", "tag": "urn:li:tag:y"},
    ]
    each = {**TAG_EACH, "params": {"entity": "{{ item.urn }}", "tag": "{{ item.tag }}"}}
    run = engine(fake_graph).run_rule(rule([each]), ctx(lookup={"urns": items}))
    assert run.steps[0].reason == "4 item(s), 2 call(s)"
    calls = mutation_calls(fake_graph)
    assert calls[0][1]["tagUrns"] == ["urn:li:tag:x"] and [r["resourceUrn"] for r in calls[0][1]["resources"]] == ["urn:li:dataset:1", "urn:li:dataset:3"]
    assert calls[1][1]["tagUrns"] == ["urn:li:tag:y"] and [r["resourceUrn"] for r in calls[1][1]["resources"]] == ["urn:li:dataset:2", "urn:li:dataset:4"]


def test_batch_context_exposes_items_and_batch_number_to_templates(fake_graph):
    items = [f"urn:li:dataset:{i}" for i in range(4)]
    each = {**TAG_EACH, "batch": {"size": 2}, "idempotencyKey": "{{ event.id }}:{{ rule.id }}:b{{ batch }}:{{ items | length }}"}
    run = engine(fake_graph).run_rule(rule([each]), ctx(lookup={"urns": items}))
    assert [r.idempotencyKey for r in run.steps[0].items] == ["evt-1:r:b1:2", "evt-1:r:b2:2"]


# ------------------------------------------------------- per-item filters ---


def test_item_when_filters_entities_and_records_skips(fake_graph):
    items = [{"urn": "urn:li:dataset:1", "type": "DATASET"}, {"urn": "urn:li:chart:2", "type": "CHART"}, {"urn": "urn:li:dataset:3", "type": "DATASET"}]
    each = {
        **TAG_EACH,
        "params": {"entity": "{{ item.urn }}", "tag": "urn:li:tag:t"},
        "itemWhen": {"operator": "AND", "filters": [{"field": "item.type", "values": ["DATASET"]}]},
    }
    run = engine(fake_graph).run_rule(rule([each]), ctx(lookup={"urns": items}))
    step = run.steps[0]
    assert step.status == "ok" and step.reason == "3 item(s), 1 skipped, 1 call(s)"
    assert [r["resourceUrn"] for r in mutation_calls(fake_graph)[0][1]["resources"]] == ["urn:li:dataset:1", "urn:li:dataset:3"]
    skipped = [r for r in step.items if r.status == "skipped"]
    assert [r.reason for r in skipped] == ["item 1: conditions not met"]


def test_item_when_on_scalar_items_or_groups_and_all_skipped(fake_graph):
    items = ["urn:li:dataset:keep", "urn:li:dataset:drop", "urn:li:chart:c"]
    each = {
        **TAG_EACH,
        "itemWhen": {
            "operator": "OR",
            "filters": [
                {"field": "item", "condition": "CONTAIN", "values": ["keep"]},
                {"field": "item", "condition": "START_WITH", "values": ["urn:li:chart:"]},
            ],
        },
    }
    run = engine(fake_graph).run_rule(rule([each]), ctx(lookup={"urns": items}))
    assert run.steps[0].reason == "3 item(s), 1 skipped, 1 call(s)"
    assert [r["resourceUrn"] for r in mutation_calls(fake_graph)[0][1]["resources"]] == ["urn:li:dataset:keep", "urn:li:chart:c"]

    fake_graph.calls.clear()
    none = {**TAG_EACH, "itemWhen": {"operator": "AND", "filters": [{"field": "item", "values": ["nothing"]}]}}
    run = engine(fake_graph).run_rule(rule([none]), ctx(lookup={"urns": items}))
    assert run.status == "ok" and run.steps[0].reason == "3 item(s), 3 skipped" and not fake_graph.calls


def test_item_when_can_use_index(fake_graph):
    each = {**TAG_EACH, "itemWhen": {"operator": "AND", "filters": [{"field": "index", "condition": "LESS_THAN", "values": ["2"]}]}}
    run = engine(fake_graph).run_rule(rule([each]), ctx(lookup={"urns": [f"urn:li:dataset:{i}" for i in range(5)]}))
    assert run.steps[0].reason == "5 item(s), 3 skipped, 1 call(s)"


# ---------------------------------------------------------- failures etc. ---


def test_one_failing_chunk_fails_the_step_but_other_chunks_still_run():
    graph = FakeGraph(fail_times=1)
    items = [f"urn:li:dataset:{i}" for i in range(4)]
    after = {"id": "after", "type": "_loop_record", "params": {"value": "ran"}}
    run = engine(graph).run_rule(rule([{**TAG_EACH, "batch": {"size": 2}}, after]), ctx(lookup={"urns": items}))
    step = run.steps[0]
    assert step.status == "failed" and [r.status for r in step.items] == ["failed", "ok"]
    assert "[batch 1: 2 item(s)] RuntimeError: transient" in step.error
    assert run.status == "failed" and len(run.steps) == 1  # onError=fail stops the rule

    graph = FakeGraph(fail_times=1)
    run = engine(graph).run_rule(rule([{**TAG_EACH, "batch": {"size": 2}, "onError": "continue"}, after]), ctx(lookup={"urns": items}))
    assert run.status == "ok" and [s.stepId for s in run.steps] == ["tag", "after"]


def test_retry_applies_per_chunk():
    graph = FakeGraph(fail_times=1)
    each = {**TAG_EACH, "batch": {"size": 2}, "retry": {"attempts": 2, "delaySeconds": 0}}
    run = engine(graph).run_rule(rule([each]), ctx(lookup={"urns": [f"urn:li:dataset:{i}" for i in range(3)]}))
    step = run.steps[0]
    assert step.status == "ok" and [r.attempts for r in step.items] == [2, 1] and step.attempts == 3


def test_item_render_errors_are_isolated_from_the_rest_of_the_batch(fake_graph):
    items = [{"urn": "urn:li:dataset:1"}, {"nope": True}, {"urn": "urn:li:dataset:3"}]
    each = {**TAG_EACH, "params": {"entity": "{{ item.urn }}", "tag": "urn:li:tag:t"}}
    run = engine(fake_graph).run_rule(rule([each]), ctx(lookup={"urns": items}))
    step = run.steps[0]
    assert step.status == "failed"
    statuses = {r.reason: r.status for r in step.items}
    assert statuses["item 1"] == "failed" and statuses["batch 1: 2 item(s)"] == "ok"
    assert [r["resourceUrn"] for r in mutation_calls(fake_graph)[0][1]["resources"]] == ["urn:li:dataset:1", "urn:li:dataset:3"]


def test_idempotency_skips_redelivered_batches_but_not_new_events(fake_graph):
    state = InMemoryStateStore()
    items = [f"urn:li:dataset:{i}" for i in range(4)]
    eng = engine(fake_graph, state=state)
    eng.run_rule(rule([{**TAG_EACH, "batch": {"size": 2}}]), ctx(lookup={"urns": items}))
    assert len(mutation_calls(fake_graph)) == 2
    again = eng.run_rule(rule([{**TAG_EACH, "batch": {"size": 2}}]), ctx(lookup={"urns": items}))
    assert [r.status for r in again.steps[0].items] == ["skipped", "skipped"] and len(mutation_calls(fake_graph)) == 2
    other = ctx(lookup={"urns": items})
    other["event"]["id"] = "evt-2"
    eng.run_rule(rule([{**TAG_EACH, "batch": {"size": 2}}]), other)
    assert len(mutation_calls(fake_graph)) == 4


def test_dry_run_batches_without_calling_datahub(fake_graph):
    items = [f"urn:li:dataset:{i}" for i in range(5)]
    run = engine(fake_graph, dry_run=True).run_rule(rule([{**TAG_EACH, "batch": {"size": 2}}]), ctx(lookup={"urns": items}))
    step = run.steps[0]
    assert run.status == "dry-run" and step.status == "dry-run" and step.reason == "5 item(s), 3 call(s)"
    assert not fake_graph.calls and all(o["dryRun"] for o in step.output)
    assert len(step.output[0]["variables"]["input"]["resources"]) == 2


def test_later_steps_see_the_fan_out_output(fake_graph):
    items = [f"urn:li:dataset:{i}" for i in range(5)]
    after = {"id": "after", "type": "_loop_record", "params": {"value": "{{ steps.tag.output | length }} calls, {{ steps.tag.status }}"}}
    run = engine(fake_graph).run_rule(rule([{**TAG_EACH, "batch": {"size": 2}}, after]), ctx(lookup={"urns": items}))
    assert run.steps[1].output["value"] == "3 calls, ok"


# --------------------------------------------------------------- end to end ---


def test_data_product_lookup_then_batched_tagging_end_to_end():
    graph = LookupGraph(250, page=100, types=lambda i: "CHART" if i % 50 == 0 else "DATASET")
    steps = [
        {"id": "assets", "type": "data_product_assets", "params": {"dataProduct": "urn:li:dataProduct:dp"}},
        {
            "id": "tag",
            "type": "add_tag",
            "forEach": "{{ steps.assets.output.entities }}",
            "itemWhen": {"operator": "AND", "filters": [{"field": "item.type", "values": ["DATASET"]}]},
            "batch": {"size": 100},
            "params": {"entity": "{{ item.urn }}", "tag": "urn:li:tag:governed"},
        },
    ]
    run = engine(graph).run_rule(rule(steps), ctx())
    assert run.status == "ok"
    lookup, tag = run.steps
    assert lookup.output["total"] == 250 and len(lookup.output["urns"]) == 250
    assert len(query_calls(graph)) == 3  # 3 pages of 100
    assert tag.reason == "250 item(s), 5 skipped, 3 call(s)"  # 245 datasets → 100 + 100 + 45
    assert [len(c[1]["resources"]) for c in mutation_calls(graph)] == [100, 100, 45]
    assert all(c[1]["tagUrns"] == ["urn:li:tag:governed"] for c in mutation_calls(graph))


def test_passing_the_whole_list_without_for_each_uses_chunked_batch_mutations():
    graph = LookupGraph(250, page=500)
    steps = [
        {"id": "assets", "type": "search", "params": {"types": ["DATASET"], "domain": "urn:li:domain:sales"}},
        {"id": "own", "type": "add_owner", "params": {"entity": "{{ steps.assets.output.urns }}", "owner": "{{ requester.urn }}"}},
    ]
    run = engine(graph).run_rule(rule(steps), ctx())
    assert run.status == "ok" and run.steps[0].output["total"] == 250
    assert [len(c[1]["resources"]) for c in mutation_calls(graph)] == [200, 50]  # BATCH_CHUNK = 200
    assert run.steps[1].output["chunks"] == 2 and run.steps[1].items is None  # one step call, no fan-out


def test_lookup_inside_a_loop_runs_per_item_and_nests_outputs():
    graph = LookupGraph(3, page=10)
    steps = [
        {"id": "assets", "type": "search", "params": {}},
        {"id": "down", "type": "lineage", "forEach": "{{ steps.assets.output.urns }}", "params": {"entity": "{{ item }}", "direction": "DOWNSTREAM", "hops": 2}},
    ]
    run = engine(graph).run_rule(rule(steps), ctx())
    down = run.steps[1]
    assert down.reason == "3 item(s)" and len(down.output) == 3
    assert all(o["urns"] == ["urn:li:dataset:999"] and o["degrees"] == {"urn:li:dataset:999": 1} for o in down.output)


def test_lookups_run_in_dry_run_but_mutations_do_not():
    graph = LookupGraph(4, page=10)
    steps = [
        {"id": "assets", "type": "data_product_assets", "params": {"dataProduct": "urn:li:dataProduct:dp"}},
        {"id": "tag", "type": "add_tag", "params": {"entity": "{{ steps.assets.output.urns }}", "tag": "urn:li:tag:t"}},
    ]
    run = engine(graph, dry_run=True).run_rule(rule(steps), ctx())
    assert run.steps[0].output["urns"] == [f"urn:li:dataset:{i}" for i in range(4)]
    assert run.steps[1].output["dryRun"] and not mutation_calls(graph)
    assert len(run.steps[1].output["variables"]["input"]["resources"]) == 4


def test_run_history_report_includes_batch_item_rows(fake_graph):
    from datahub_workflow_actions.runs import steps_report

    items = [f"urn:li:dataset:{i}" for i in range(3)]
    run = engine(fake_graph).run_rule(rule([{**TAG_EACH, "batch": {"size": 2}}]), ctx(lookup={"urns": items}))
    report = json.loads(steps_report(run.steps))  # steps_report returns compact JSON
    assert report[0]["items"] == [{"status": "ok", "error": None}, {"status": "ok", "error": None}]


# ----------------------------------------------------------------- contract ---


@pytest.mark.parametrize("batch", [{"size": 0}, {"size": 1001}, {"mode": "sometimes"}, {"chunk": 5}])
def test_batch_policy_rejects_invalid_values(batch):
    with pytest.raises(ValueError):
        rule([{**TAG_EACH, "batch": batch}])


def test_item_when_must_be_a_filter_group():
    with pytest.raises(ValueError):
        rule([{**TAG_EACH, "itemWhen": [{"field": "item"}]}])


def test_schema_and_catalog_publish_the_new_fields():
    from datahub_workflow_actions.contract import rules_json_schema

    step_props = rules_json_schema()["$defs"]["Step"]["properties"]
    assert "itemWhen" in step_props and "batch" in step_props
    by_type = {c["type"]: c for c in catalog()}
    metadata = [t for t, c in by_type.items() if c["group"] == "Metadata"]
    assert len(metadata) == 13 and all(by_type[t]["bulkParam"] == "entity" for t in metadata)
    assert all(by_type[t]["bulkParam"] is None for t in by_type if by_type[t]["group"] in ("Lookup", "Integration"))
    assert {t for t, c in by_type.items() if c["group"] == "Lookup"} == {"search", "data_product_assets", "lineage", "graphql"}


def test_rules_config_round_trips_loops_and_batches():
    config = RulesConfig.model_validate({"schemaVersion": 1, "rules": [rule([{**TAG_EACH, "batch": {"size": 5}, "itemWhen": {"operator": "AND", "filters": []}}]).model_dump()]})
    dumped = config.model_dump()["rules"][0]["steps"][0]
    assert dumped["batch"] == {"mode": "auto", "size": 5} and dumped["forEach"] == "{{ lookup.urns }}"
