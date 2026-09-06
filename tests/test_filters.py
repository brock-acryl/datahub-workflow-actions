from datahub_workflow_actions.contract import Filter, FilterGroup
from datahub_workflow_actions.filters import evaluate, resolve_path

CTX = {
    "form": {"field_abc": "restricted", "field_num": "42", "field_date": "2026-01-15"},
    "entity": {"type": "dataset", "owners": ["urn:li:corpuser:a", "urn:li:corpGroup:g"], "tags": []},
    "requester": {"groups": ["urn:li:corpGroup:analysts"]},
    "decisions": [{"result": "ACCEPTED", "note": "ok"}, {"result": "REJECTED", "note": None}],
    "event": {"time": 1754000000000},
}


def f(field, condition="EQUAL", values=None, **kw):
    return Filter(field=field, condition=condition, values=values if values is not None else ["x"], **kw)


def test_resolve_path_maps_over_lists():
    assert resolve_path(CTX, "decisions.result") == (True, ["ACCEPTED", "REJECTED"])
    assert resolve_path(CTX, "decisions.0.result") == (True, "ACCEPTED")
    assert resolve_path(CTX, "entity.missing") == (False, None)
    assert resolve_path(CTX, "decisions.5.result") == (False, None)


def test_operators():
    assert evaluate(f("form.field_abc", "EQUAL", ["restricted"]), CTX)
    assert not evaluate(f("form.field_abc", "EQUAL", ["Restricted"]), CTX)
    assert evaluate(f("form.field_abc", "EQUAL", ["Restricted"], caseInsensitive=True), CTX)
    assert evaluate(f("form.field_abc", "CONTAIN", ["strict"]), CTX)
    assert evaluate(f("form.field_abc", "START_WITH", ["res"]), CTX)
    assert evaluate(f("form.field_abc", "END_WITH", ["ted"]), CTX)
    assert evaluate(f("form.field_abc", "IN", ["public", "restricted"]), CTX)
    assert evaluate(f("form.field_abc", "MATCHES", ["^re.*ed$"]), CTX)
    assert evaluate(f("entity.type", "EXISTS", []), CTX)
    assert not evaluate(f("entity.tags", "EXISTS", []), CTX)
    assert not evaluate(f("entity.nope", "EXISTS", []), CTX)
    assert evaluate(f("form.field_abc", "EQUAL", ["public"], negated=True), CTX)


def test_typed_comparisons_numbers_and_dates():
    assert evaluate(f("form.field_num", "GREATER_THAN", ["41"]), CTX)
    assert not evaluate(f("form.field_num", "GREATER_THAN", ["42"]), CTX)
    assert evaluate(f("form.field_num", "LESS_THAN", ["100"]), CTX)
    assert evaluate(f("form.field_date", "GREATER_THAN", ["2025-12-31"]), CTX)
    assert evaluate(f("form.field_date", "LESS_THAN", ["2026-02-01T00:00:00Z"]), CTX)
    assert not evaluate(f("form.field_abc", "GREATER_THAN", ["1"]), CTX)  # string vs number → no match
    assert evaluate(f("event.time", "GREATER_THAN", ["1700000000000"]), CTX)


def test_list_values_match_any_element():
    assert evaluate(f("entity.owners", "EQUAL", ["urn:li:corpGroup:g"]), CTX)
    assert evaluate(f("requester.groups", "CONTAIN", ["analysts"]), CTX)
    assert evaluate(f("decisions.result", "EQUAL", ["REJECTED"]), CTX)
    assert not evaluate(f("entity.owners", "EQUAL", ["urn:li:corpuser:zzz"]), CTX)


def test_nested_groups_and_empty_group_is_always():
    assert evaluate(None, CTX)
    assert evaluate(FilterGroup(operator="AND", filters=[]), CTX)
    group = FilterGroup(
        operator="OR",
        filters=[
            FilterGroup(operator="AND", filters=[f("form.field_abc", "EQUAL", ["restricted"]), f("entity.type", "EQUAL", ["chart"])]),
            f("requester.groups", "CONTAIN", ["analysts"]),
        ],
    )
    assert evaluate(group, CTX)  # second branch
    group_and = FilterGroup(operator="AND", filters=[f("form.field_abc", "EQUAL", ["restricted"]), f("entity.type", "EQUAL", ["chart"])])
    assert not evaluate(group_and, CTX)
