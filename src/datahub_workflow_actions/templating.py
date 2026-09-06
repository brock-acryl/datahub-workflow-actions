"""Jinja2 rendering of step parameters over the context document.

Sandboxed, ``StrictUndefined`` (a typo in ``{{ form.reasn }}`` fails loudly
instead of rendering ''), plus a few filters. A value that is exactly one
expression (``"{{ entity.owners }}"``) renders to the *object*, not its
string form, so lists and dicts survive for ``forEach`` and JSON bodies."""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from typing import Any, Mapping

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


def _environment() -> SandboxedEnvironment:
    env = SandboxedEnvironment(undefined=StrictUndefined, autoescape=False, keep_trailing_newline=True)
    env.filters["urn_name"] = urn_name
    env.filters["date"] = fmt_date
    env.filters["json"] = to_json
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
