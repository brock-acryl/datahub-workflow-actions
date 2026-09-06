"""``sql`` step — run templated statements against a named SQLAlchemy connection.

Identifiers can't be bound, so statements are Jinja templates; every
``{{ }}`` inside them must go through a ``sql_*`` filter (``sql_ident``,
``sql_literal``, ``sql_table``, ``sql_schema``, ``sql_database``) unless the
step opts into ``unsafeRawTemplates``. Values that *can* be bound go in
``parameters`` (``:name`` style). Connections are declared once in the action
config (``connections: {warehouse: ${SNOWFLAKE_URL}}``) and referenced by
name, so no credential ever sits in a rule."""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Union

from pydantic import Field

from datahub_workflow_actions.steps import RunContext, StepParams, step
from datahub_workflow_actions.templating import find_unsafe_sql_expressions, set_sql_dialect

Statements = Union[str, List[str]]


class SqlParams(StepParams):
    connection: str = Field(description="Name of a connection declared in the action config.")
    statements: Statements = Field(description="One statement, or a list run in order. Wrap identifiers with sql_table / sql_ident.")
    parameters: Optional[Dict[str, Any]] = Field(None, description="Bound parameters (:name) for values that can be bound.")
    transaction: bool = Field(True, description="Run all statements in one transaction.")
    maxRows: int = Field(100, ge=0, le=10000, description="Rows kept in the output for SELECT-like statements.")
    unsafeRawTemplates: bool = Field(False, description="Allow {{ }} expressions without a sql_* filter (not recommended).")


def _statements(value: Statements) -> List[str]:
    items = [value] if isinstance(value, str) else list(value)
    return [s.strip().rstrip(";") for s in items if s and s.strip()]


def validate_sql_template(raw_params: Dict[str, Any]) -> List[str]:
    if raw_params.get("unsafeRawTemplates") in (True, "true", "True"):
        return []
    problems: List[str] = []
    for statement in _statements(raw_params.get("statements") or []):
        for expression in find_unsafe_sql_expressions(statement):
            problems.append(f"unsafe template {expression} — wrap it with sql_table / sql_ident / sql_literal (or set unsafeRawTemplates)")
    return problems


def _dialect_of(url: str) -> str:
    return url.split(":", 1)[0].split("+", 1)[0].lower() if url else "ansi"


@step(
    "sql",
    label="Run SQL",
    description="Execute templated SQL (e.g. GRANT) on a named connection.",
    group="Integration",
    params=SqlParams,
    outputs={"rowcount": "Rows affected by the last statement", "rows": "Rows returned by the last statement (capped)", "statements": "Statements as executed"},
    validate_template=validate_sql_template,
)
def sql(p: SqlParams, ctx: RunContext) -> dict:
    url = ctx.connections.get(p.connection)
    if not url and not ctx.dry_run:
        raise ValueError(f"sql: unknown connection '{p.connection}' — declare it under connections in the action config")
    set_sql_dialect(_dialect_of(url or ""))
    statements = _statements(p.statements)
    if not statements:
        raise ValueError("sql: no statements")
    if ctx.dry_run:
        return {"dryRun": True, "connection": p.connection, "dialect": _dialect_of(url or ""), "statements": statements, "parameters": p.parameters or {}}
    try:
        from sqlalchemy import create_engine, text
    except ImportError as e:  # pragma: no cover
        raise RuntimeError("sql step needs SQLAlchemy: pip install 'datahub-workflow-actions[sql]' plus the driver") from e

    engine = create_engine(url)
    rowcount: Optional[int] = None
    rows: List[Any] = []
    try:
        with engine.connect() as connection:
            def run_all(conn):
                nonlocal rowcount, rows
                for statement in statements:
                    result = conn.execute(text(statement), p.parameters or {})
                    rowcount = result.rowcount if result.rowcount is not None and result.rowcount >= 0 else None
                    rows = [dict(row._mapping) for row in result.fetchmany(p.maxRows)] if result.returns_rows else []

            if p.transaction:
                with connection.begin():
                    run_all(connection)
            else:
                run_all(connection)
                if hasattr(connection, "commit"):
                    connection.commit()
    finally:
        engine.dispose()
    return {"rowcount": rowcount, "rows": rows, "statements": statements}
