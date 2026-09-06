"""Evaluate FilterGroups against the context document.

Paths are dotted (``form.field_abc``, ``entity.owners``); a segment applied to
a list maps over it, so ``decisions.result`` yields every decision result and
a condition matches if *any* element matches. Comparisons coerce numbers and
ISO/epoch dates for GREATER_THAN / LESS_THAN."""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any, List, Mapping, Optional, Tuple

from datahub_workflow_actions.contract import Filter, FilterGroup, FilterNode

_MISSING = object()


def resolve_path(context: Mapping[str, Any], path: str) -> Tuple[bool, Any]:
    """Returns (found, value). ``found`` is False when any segment is absent."""
    current: Any = context
    for segment in [s for s in path.split(".") if s]:
        if isinstance(current, list):
            if segment.isdigit():
                index = int(segment)
                if index >= len(current):
                    return False, None
                current = current[index]
                continue
            mapped = []
            for element in current:
                found, value = _get(element, segment)
                if found:
                    mapped.extend(value if isinstance(value, list) else [value])
            if not mapped:
                return False, None
            current = mapped
            continue
        found, current = _get(current, segment)
        if not found:
            return False, None
    return True, current


def _get(container: Any, key: str) -> Tuple[bool, Any]:
    if isinstance(container, Mapping):
        if key in container:
            return True, container[key]
        return False, None
    if hasattr(container, key):
        return True, getattr(container, key)
    return False, None


def _coerce(value: Any) -> Any:
    """Number if it looks like one, else timestamp (epoch ms / ISO) as a datetime, else str."""
    if isinstance(value, bool):
        return str(value).lower()
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    try:
        return float(text)
    except ValueError:
        pass
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(timezone.utc)
    except ValueError:
        return text


def _matches_one(actual: Any, filter_: Filter) -> bool:
    condition = filter_.condition
    values = [v.strip() for v in filter_.values]
    text = "" if actual is None else str(actual)
    if filter_.caseInsensitive:
        text = text.lower()
        values = [v.lower() for v in values]
    if condition == "EQUAL":
        return any(text == v for v in values)
    if condition == "IN":
        return text in values
    if condition == "CONTAIN":
        return any(v in text for v in values)
    if condition == "START_WITH":
        return any(text.startswith(v) for v in values)
    if condition == "END_WITH":
        return any(text.endswith(v) for v in values)
    if condition == "MATCHES":
        flags = re.I if filter_.caseInsensitive else 0
        return any(re.search(v, "" if actual is None else str(actual), flags) for v in values)
    if condition in ("GREATER_THAN", "LESS_THAN"):
        left = _coerce(actual)
        for v in values:
            right = _coerce(v)
            if type(left) is not type(right):
                continue
            try:
                if (left > right) if condition == "GREATER_THAN" else (left < right):
                    return True
            except TypeError:
                continue
        return False
    raise ValueError(f"unknown condition {condition}")


def evaluate_filter(filter_: Filter, context: Mapping[str, Any]) -> bool:
    found, value = resolve_path(context, filter_.field)
    if filter_.condition == "EXISTS":
        result = found and value not in (None, "", [], {})
    elif not found:
        result = False
    else:
        candidates: List[Any] = value if isinstance(value, list) else [value]
        result = any(_matches_one(candidate, filter_) for candidate in candidates)
    return (not result) if filter_.negated else result


def evaluate(group: Optional[FilterNode], context: Mapping[str, Any]) -> bool:
    """An absent or empty group is ``True`` ("always")."""
    if group is None:
        return True
    if isinstance(group, Filter):
        return evaluate_filter(group, context)
    if not group.filters:
        return True
    results = (evaluate(child, context) for child in group.filters)
    return all(results) if group.operator == "AND" else any(results)
