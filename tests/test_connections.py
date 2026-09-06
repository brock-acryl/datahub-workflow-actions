import json

import pytest

from datahub_workflow_actions.connections import (
    ConnectionError_, ConnectionResolver, parse_connections, url_from_recipe, url_has_literal_password, validate_connections,
)
from datahub_workflow_actions.steps import RunContext, get_step
from tests.conftest import DATASET

SNOW_URN = "urn:li:dataHubIngestionSource:snow"
CLI_URN = "urn:li:dataHubIngestionSource:cli"


class FakeGraph:
    """Answers ingestionSource / ingestionSourceForEntity; records secret lookups."""

    def __init__(self):
        self.calls = []
        self.sources = {
            SNOW_URN: {
                "urn": SNOW_URN, "name": "Snowflake prod", "type": "snowflake",
                "config": {"executorId": "default", "recipe": json.dumps({"source": {"type": "snowflake", "config": {
                    "account_id": "xy1.us-east-1", "username": "svc", "password": "${SNOW_PW}", "warehouse": "WH", "role": "R"}}})},
            },
            CLI_URN: {"urn": CLI_URN, "type": "postgres", "config": {"executorId": "__datahub_cli_", "recipe": "{}"}},
        }

    def execute_graphql(self, query, variables=None):
        self.calls.append((query.split("(")[0].split()[-1], variables))
        urn = variables["urn"]
        if "ingestionSourceForEntity" in query:
            return {"ingestionSourceForEntity": self.sources[SNOW_URN] if urn == DATASET else None}
        return {"ingestionSource": self.sources.get(urn)}


def test_parse_accepts_strings_and_the_three_kinds():
    specs = parse_connections({
        "a": "postgresql://u:${P}@h/db",
        "b": {"ingestionSource": SNOW_URN, "description": "prod"},
        "c": {"fromEntity": True, "platform": "snowflake"},
        "d": {"url": "sqlite://", "platform": "other"},
        "junk": 42,
    })
    assert [(n, s.kind) for n, s in specs.items()] == [("a", "url"), ("b", "ingestionSource"), ("c", "fromEntity"), ("d", "url")]
    assert specs["b"].description == "prod" and specs["c"].platform == "snowflake"


def test_validation_flags_bad_specs_and_literal_passwords():
    assert url_has_literal_password("postgresql://u:hunter2@h/db")
    assert not url_has_literal_password("postgresql://u:${PG}@h/db")
    assert not url_has_literal_password("bigquery://proj")
    problems = validate_connections({
        "p": {"url": "postgresql://u:hunter2@h/db"},
        "q": {"ingestionSource": "urn:li:dataset:x"},
        "r": {"url": "host/db"},
        "s": {},
        "ok": {"fromEntity": True},
    })
    assert problems == [
        "connection 'p': the password must be a ${SECRET} reference, not a literal value",
        "connection 'q': ingestionSource must be a dataHubIngestionSource urn",
        "connection 'r': url must start with a SQLAlchemy scheme (e.g. postgresql://)",
        "connection 's': url is required",
    ]


def test_url_from_recipe_per_platform():
    url, kw = url_from_recipe("postgres", {"host_port": "db:5432", "database": "reports", "username": "u", "password": "p w"})
    assert url == "postgresql+psycopg2://u:p+w@db:5432/reports" and kw == {}
    url, _ = url_from_recipe("redshift", {"sqlalchemy_uri": "redshift+psycopg2://u:p@rs/dw"})
    assert url == "redshift+psycopg2://u:p@rs/dw"
    url, _ = url_from_recipe("unity-catalog", {"workspace_url": "https://adb.example.com", "token": "tok", "warehouse_id": "abc"})
    assert url == "databricks://token:tok@adb.example.com?http_path=%2Fsql%2F1.0%2Fwarehouses%2Fabc"
    url, kw = url_from_recipe("bigquery", {"project_on_behalf": "proj", "credential": {"project_id": "proj", "private_key": "k"}})
    assert url == "bigquery://proj" and kw["credentials_info"]["type"] == "service_account"
    snow, _ = url_from_recipe("snowflake", {"account_id": "acct", "username": "svc", "password": "pw", "database": "DB", "warehouse": "WH"})
    assert snow.startswith("snowflake://svc:pw@acct") and "warehouse=WH" in snow
    with pytest.raises(ConnectionError_):
        url_from_recipe("kafka", {})
    with pytest.raises(ConnectionError_):
        url_from_recipe("unity-catalog", {"workspace_url": "https://x", "token": "t"})


def test_resolver_reuses_an_ingestion_source_and_resolves_secrets_from_env(monkeypatch):
    monkeypatch.setenv("SNOW_PW", "s3cret")
    graph = FakeGraph()
    resolver = ConnectionResolver.from_config({"snow": {"ingestionSource": SNOW_URN}, "entity": {"fromEntity": True}}, graph=graph)
    resolved = resolver.resolve("snow")
    assert resolved.kind == "ingestionSource" and resolved.source_urn == SNOW_URN and resolved.source_type == "snowflake"
    assert resolved.dialect == "snowflake" and "s3cret" in resolved.url and "${" not in resolved.url
    assert resolved.describe() == {"connection": "snow", "kind": "ingestionSource", "dialect": "snowflake", "ingestionSource": SNOW_URN, "sourceType": "snowflake"}
    # cached: a second resolve doesn't hit the graph again
    calls = len(graph.calls)
    resolver.resolve("snow")
    assert len(graph.calls) == calls

    by_entity = resolver.resolve("entity", {"entity": {"urn": DATASET}})
    assert by_entity.kind == "fromEntity" and by_entity.source_urn == SNOW_URN
    with pytest.raises(ConnectionError_, match="no entity urn"):
        resolver.resolve("entity", {})
    with pytest.raises(ConnectionError_, match="no ingestion source found"):
        resolver.resolve("entity", {"entity": {"urn": "urn:li:dataset:(urn:li:dataPlatform:x,other,PROD)"}})


def test_resolver_refuses_cli_sources_and_unknown_names():
    resolver = ConnectionResolver.from_config({"cli": {"ingestionSource": CLI_URN}}, graph=FakeGraph())
    with pytest.raises(ConnectionError_, match="CLI-managed"):
        resolver.resolve("cli")
    with pytest.raises(ConnectionError_, match="unknown connection"):
        resolver.resolve("nope")
    with pytest.raises(ConnectionError_, match="needs a DataHub connection"):
        ConnectionResolver.from_config({"snow": {"ingestionSource": SNOW_URN}}).resolve("snow")


def test_sql_step_dry_run_describes_every_kind_without_touching_secrets():
    graph = FakeGraph()
    resolver = ConnectionResolver.from_config(
        {"snow": {"ingestionSource": SNOW_URN}, "entity": {"fromEntity": True, "platform": "snowflake"}, "pg": "postgresql://u:${P}@h/db"},
        graph=graph,
    )
    d = get_step("sql")
    ctx = RunContext(dry_run=True, connection_resolver=resolver, context={"entity": {"urn": DATASET}})
    out = d.run(d.params.model_validate({"connection": "entity", "statements": ["GRANT SELECT ON {{ entity.urn | sql_table }} TO ROLE x"]}), ctx)
    assert out["dryRun"] and out["kind"] == "fromEntity" and out["entity"] == DATASET and out["platform"] == "snowflake"
    out = d.run(d.params.model_validate({"connection": "snow", "statements": "SELECT 1"}), ctx)
    assert out["kind"] == "ingestionSource" and out["ingestionSource"] == SNOW_URN
    out = d.run(d.params.model_validate({"connection": "pg", "statements": "SELECT 1"}), ctx)
    assert out["kind"] == "url" and out["dialect"] == "postgresql"
    assert graph.calls == []  # dry runs never fetch recipes or secrets
    with pytest.raises(ValueError, match="unknown connection"):
        d.run(d.params.model_validate({"connection": "nope", "statements": "SELECT 1"}), ctx)


def test_sql_step_executes_through_a_resolved_url_connection():
    d = get_step("sql")
    ctx = RunContext(connection_resolver=ConnectionResolver.from_config({"mem": {"url": "sqlite://", "platform": "other"}}))
    out = d.run(d.params.model_validate({"connection": "mem", "statements": ["SELECT 1 AS one"]}), ctx)
    assert out["rows"] == [{"one": 1}] and out["kind"] == "url" and out["dialect"] == "sqlite"


def test_validate_cli_checks_connection_specs_and_references(tmp_path, capsys):
    from datahub_workflow_actions.cli import main

    recipe = {
        "source": {"type": "datahub-workflow-actions", "config": {
            "schemaVersion": 1,
            "connections": {"warehouse": {"ingestionSource": SNOW_URN}, "bad": {"url": "postgresql://u:pw@h/db"}},
            "rules": [{"id": "r", "name": "r", "workflowUrn": "urn:li:actionWorkflow:w", "on": {"operation": "COMPLETED", "result": "ACCEPTED"},
                       "steps": [{"id": "s", "type": "sql", "params": {"connection": "nope", "statements": ["SELECT 1"]}}]}],
        }}
    }
    path = tmp_path / "recipe.json"
    path.write_text(json.dumps(recipe))
    assert main(["validate", str(path)]) == 1
    err = capsys.readouterr().err
    assert "connection 'bad': the password must be a ${SECRET} reference" in err
    assert "r/s: connection 'nope' is not declared under connections (bad, warehouse)" in err

    recipe["source"]["config"]["connections"].pop("bad")
    recipe["source"]["config"]["rules"][0]["steps"][0]["params"]["connection"] = "warehouse"
    path.write_text(json.dumps(recipe))
    assert main(["validate", str(path)]) == 0
