"""Jinja2 rendering of step parameters over the context document.

Sandboxed, ``StrictUndefined`` (a typo in ``{{ form.reasn }}`` fails loudly
instead of rendering ''), plus a few filters. A value that is exactly one
expression (``"{{ entity.owners }}"``) renders to the *object*, not its
string form, so lists and dicts survive for ``forEach`` and JSON bodies."""

from __future__ import annotations

import contextvars
import json
import re
from datetime import datetime, timezone
from typing import Any, List, Mapping

from jinja2 import StrictUndefined, TemplateError as _JinjaTemplateError, Undefined, UndefinedError
from jinja2.sandbox import SandboxedEnvironment

_SINGLE_EXPRESSION = re.compile(r"^\s*\{\{([^{}]*)\}\}\s*$", re.S)
_HAS_TEMPLATE = re.compile(r"\{[\{%]")


class TemplateError(ValueError):
    pass


def urn_name(value: Any) -> str:
    """Human tail of a URN: dataset urns yield the table name, others the id."""
    if value is None:
        return ""
    urn = str(value)
    if "(" in urn and urn.endswith(")"):
        inner = urn[urn.index("(") + 1 : -1]
        parts = [p for p in inner.split(",") if p]
        # dataset: (platform,name,env); dataFlow: (orchestrator,id,env); dataJob: (flowUrn,id)
        if len(parts) >= 2:
            candidate = parts[1] if len(parts) >= 3 or parts[0].startswith("urn:") else parts[-1]
            return candidate.strip()
        return inner
    return urn.rsplit(":", 1)[-1]


def fmt_date(value: Any, fmt: str = "%Y-%m-%d") -> str:
    if value in (None, ""):
        return ""
    if isinstance(value, (int, float)):
        seconds = value / 1000 if value > 1e11 else value
        return datetime.fromtimestamp(seconds, tz=timezone.utc).strftime(fmt)
    text = str(value)
    if text.isdigit():
        return fmt_date(int(text), fmt)
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).strftime(fmt)
    except ValueError:
        return text


def to_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, default=str)


# ---- SQL helpers ------------------------------------------------------------
# Identifiers cannot be bound as SQL parameters, so GRANT-style statements are
# templated. These filters make that safe: identifiers are quoted for the
# dialect (quotes doubled), literals are single-quoted with escaping, and
# dataset URNs decompose into db.schema.table parts. The `sql` step sets the
# dialect from the connection URL before rendering.
SQL_DIALECT: contextvars.ContextVar[str] = contextvars.ContextVar("sql_dialect", default="ansi")
_BACKTICK_DIALECTS = {"bigquery", "databricks", "mysql", "hive", "spark", "trino-backtick"}
SQL_FILTER_NAMES = ("sql_ident", "sql_literal", "sql_table", "sql_schema", "sql_database")


def set_sql_dialect(name: str) -> None:
    SQL_DIALECT.set((name or "ansi").split("+", 1)[0].lower())


def _quote_char() -> str:
    return "`" if SQL_DIALECT.get() in _BACKTICK_DIALECTS else '"'


def _quote_one(part: str) -> str:
    q = _quote_char()
    text = str(part)
    if not text:
        raise TemplateError("sql_ident: empty identifier")
    if "\x00" in text or ";" in text:
        raise TemplateError(f"sql_ident: illegal characters in identifier {text!r}")
    return f"{q}{text.replace(q, q + q)}{q}"


def sql_ident(value: Any) -> str:
    """Quote an identifier (dotted names quote each part)."""
    if value is None:
        raise TemplateError("sql_ident: identifier is undefined")
    return ".".join(_quote_one(part) for part in str(value).split("."))


def sql_literal(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return str(value)
    return "'" + str(value).replace("'", "''") + "'"


def _dataset_parts(value: Any) -> List[str]:
    name = urn_name(value) if str(value).startswith("urn:") else str(value)
    parts = [p for p in name.split(".") if p]
    if not parts:
        raise TemplateError(f"sql_table: cannot derive a table name from {value!r}")
    return parts


def sql_table(value: Any) -> str:
    return ".".join(_quote_one(p) for p in _dataset_parts(value))


def sql_schema(value: Any) -> str:
    parts = _dataset_parts(value)
    if len(parts) < 2:
        raise TemplateError(f"sql_schema: {value!r} has no schema part")
    return ".".join(_quote_one(p) for p in parts[:-1])


def sql_database(value: Any) -> str:
    parts = _dataset_parts(value)
    if len(parts) < 3:
        raise TemplateError(f"sql_database: {value!r} has no database part")
    return _quote_one(parts[0])


_EXPRESSION = re.compile(r"\{\{(.*?)\}\}", re.S)


def find_unsafe_sql_expressions(statement: str) -> List[str]:
    """Expressions in a SQL template that are not wrapped in a sql_* filter."""
    unsafe = []
    for match in _EXPRESSION.finditer(statement or ""):
        expression = match.group(1)
        if not any(f"| {name}" in expression or f"|{name}" in expression for name in SQL_FILTER_NAMES):
            unsafe.append("{{" + expression + "}}")
    return unsafe


def _environment() -> SandboxedEnvironment:
    env = SandboxedEnvironment(undefined=StrictUndefined, autoescape=False, keep_trailing_newline=True)
    env.filters["urn_name"] = urn_name
    env.filters["date"] = fmt_date
    env.filters["json"] = to_json
    env.filters["sql_ident"] = sql_ident
    env.filters["sql_literal"] = sql_literal
    env.filters["sql_table"] = sql_table
    env.filters["sql_schema"] = sql_schema
    env.filters["sql_database"] = sql_database
    return env


ENV = _environment()


def is_template(value: Any) -> bool:
    return isinstance(value, str) and bool(_HAS_TEMPLATE.search(value))


def render_string(template: str, context: Mapping[str, Any]) -> str:
    try:
        return ENV.from_string(template).render(**context)
    except UndefinedError as e:
        raise TemplateError(f"{e} in template {template!r}") from e
    except _JinjaTemplateError as e:
        raise TemplateError(f"{e} in template {template!r}") from e


def render_value(value: Any, context: Mapping[str, Any]) -> Any:
    """Render recursively. A string that is a single ``{{ expr }}`` returns
    the evaluated object; anything else renders to a string."""
    if isinstance(value, str):
        if not is_template(value):
            return value
        single = _SINGLE_EXPRESSION.match(value)
        if single:
            try:
                result = ENV.compile_expression(single.group(1), undefined_to_none=False)(**context)
                if isinstance(result, Undefined):
                    # StrictUndefined only raises when used; a bare undefined expression must fail too.
                    str(result)
                return result
            except UndefinedError as e:
                raise TemplateError(f"{e} in template {value!r}") from e
            except _JinjaTemplateError as e:
                raise TemplateError(f"{e} in template {value!r}") from e
        return render_string(value, context)
    if isinstance(value, list):
        return [render_value(v, context) for v in value]
    if isinstance(value, dict):
        return {k: render_value(v, context) for k, v in value.items()}
    return value


def render_params(params: Mapping[str, Any], context: Mapping[str, Any]) -> dict:
    return {key: render_value(value, context) for key, value in params.items()}
