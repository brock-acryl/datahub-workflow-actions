import pytest

from datahub_workflow_actions.steps import RunContext, get_step
from datahub_workflow_actions.steps.sql import validate_sql_template
from datahub_workflow_actions.templating import (
    TemplateError, find_unsafe_sql_expressions, render_value, set_sql_dialect, sql_database, sql_ident, sql_literal, sql_schema, sql_table,
)
from tests.conftest import DATASET


def run(type_, params, ctx):
    d = get_step(type_)
    return d.run(d.params.model_validate(params), ctx)


def test_sql_filters_quote_for_the_dialect():
    set_sql_dialect("snowflake")
    assert sql_table(DATASET) == '"db"."sales"."orders"'
    assert sql_schema(DATASET) == '"db"."sales"'
    assert sql_database(DATASET) == '"db"'
    assert sql_ident('weird"name') == '"weird""name"'
    assert sql_ident("analyst_role") == '"analyst_role"'
    assert sql_literal("O'Brien") == "'O''Brien'"
    assert sql_literal(3) == "3" and sql_literal(None) == "NULL" and sql_literal(True) == "TRUE"
    set_sql_dialect("bigquery")
    assert sql_table("proj.ds.tbl") == "`proj`.`ds`.`tbl`"
    set_sql_dialect("postgresql+psycopg2")
    assert sql_table("public.orders") == '"public"."orders"'
    with pytest.raises(TemplateError):
        sql_ident("a; drop table x")
    with pytest.raises(TemplateError):
        sql_database("schema.table")


def test_templates_render_grants_safely():
    set_sql_dialect("snowflake")
    ctx = {"entity": {"urn": DATASET}, "form": {"field_role": 'ANALYST"role'}}
    out = render_value("GRANT SELECT ON TABLE {{ entity.urn | sql_table }} TO ROLE {{ form.field_role | sql_ident }}", ctx)
    assert out == 'GRANT SELECT ON TABLE "db"."sales"."orders" TO ROLE "ANALYST""role"'
    # a value smuggling a statement terminator is refused outright rather than quoted
    with pytest.raises(TemplateError):
        render_value("GRANT SELECT ON t TO {{ form.field_role | sql_ident }}", {"form": {"field_role": 'x"; DROP TABLE y; --'}})


def test_unsafe_expressions_are_detected():
    assert find_unsafe_sql_expressions("GRANT SELECT ON {{ entity.urn | sql_table }} TO {{ form.role }}") == ["{{ form.role }}"]
    assert find_unsafe_sql_expressions("SELECT 1") == []
    assert validate_sql_template({"statements": ["GRANT x TO {{ form.role }}"]}) == [
        "unsafe template {{ form.role }} — wrap it with sql_table / sql_ident / sql_literal (or set unsafeRawTemplates)"
    ]
    assert validate_sql_template({"statements": ["GRANT x TO {{ form.role }}"], "unsafeRawTemplates": True}) == []
    assert validate_sql_template({"statements": "GRANT x TO {{ form.role | sql_ident }}"}) == []


def test_sql_step_runs_against_sqlite_with_bound_params_and_outputs():
    ctx = RunContext(connections={"warehouse": "sqlite://"})
    # one connection per step run — create + insert + select in a single transaction
    out = run(
        "sql",
        {
            "connection": "warehouse",
            "statements": ["CREATE TABLE grants (role TEXT, tbl TEXT)", "INSERT INTO grants VALUES (:role, :tbl)", "SELECT role, tbl FROM grants"],
            "parameters": {"role": "analyst", "tbl": "orders"},
        },
        ctx,
    )
    assert out["rows"] == [{"role": "analyst", "tbl": "orders"}] and out["statements"][0].startswith("CREATE TABLE")


def test_sql_step_dry_run_and_unknown_connection():
    out = run("sql", {"connection": "warehouse", "statements": "GRANT SELECT ON t TO r"}, RunContext(dry_run=True, connections={"warehouse": "snowflake://x"}))
    assert out == {"dryRun": True, "connection": "warehouse", "dialect": "snowflake", "statements": ["GRANT SELECT ON t TO r"], "parameters": {}}
    with pytest.raises(ValueError, match="unknown connection"):
        run("sql", {"connection": "nope", "statements": "SELECT 1"}, RunContext(connections={}))


def test_engine_rejects_unsafe_sql_before_rendering(context):
    from datahub_workflow_actions.contract import RulesConfig
    from datahub_workflow_actions.engine import Engine
    from tests.conftest import WF

    cfg = RulesConfig(rules=[{
        "id": "r", "workflowUrn": WF, "on": {"operation": "COMPLETED", "result": "ACCEPTED"},
        "steps": [{"id": "grant", "type": "sql", "params": {"connection": "warehouse", "statements": ["GRANT SELECT ON {{ entity.urn | sql_table }} TO {{ requester.username }}"]}}],
    }])
    run_ = Engine(RunContext(connections={"warehouse": "sqlite://"})).run(cfg, context)[0]
    assert run_.steps[0].status == "failed" and "unsafe template {{ requester.username }}" in run_.steps[0].error


def test_connections_normalize_env_placeholders(monkeypatch):
    from datahub_workflow_actions.action import normalize_connections

    monkeypatch.setenv("WH_URL", "snowflake://u:p@acct/db")
    assert normalize_connections({"warehouse": "${WH_URL}", "pg": {"url": "postgresql://x"}, "empty": None}) == {
        "warehouse": "snowflake://u:p@acct/db", "pg": "postgresql://x",
    }
