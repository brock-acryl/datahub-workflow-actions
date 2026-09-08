"""§21 E3 — volume controls in front of the rule engine.

``RecentEvents`` drops near-duplicate change events: GMS emits the same TAG ADD twice when a
tag lives in both ``schemaMetadata`` and ``editableSchemaMetadata``, and re-ingestion replays
unchanged tags. Two events with the same identity (entity, category, operation, modifier) inside
the window count as one.

``RuleRateLimiter`` caps how often one rule may fire per minute (token bucket), so a burst of
changes — a bulk ingest, a mass tag — cannot fan a rule out into thousands of runs. Both are
hot-reloadable (`dedupeWindowSeconds`, `limits.maxRunsPerRulePerMinute`)."""

from __future__ import annotations

import time
from collections import OrderedDict
from typing import Any, Callable, Dict, Mapping, Optional

DEFAULT_DEDUPE_WINDOW_SECONDS = 30.0
MAX_TRACKED = 10000


def event_identity(view: Mapping[str, Any]) -> str:
    """Everything that makes a change *the same change*, minus its timestamp."""
    return ":".join(
        str(view.get(key) if view.get(key) is not None else "") for key in ("entityUrn", "category", "operation", "modifier")
    )


class RecentEvents:
    def __init__(self, window_seconds: float = DEFAULT_DEDUPE_WINDOW_SECONDS, clock: Callable[[], float] = time.monotonic):
        self.window = max(0.0, float(window_seconds))
        self.clock = clock
        self._seen: "OrderedDict[str, float]" = OrderedDict()

    @property
    def enabled(self) -> bool:
        return self.window > 0

    def duplicate(self, view: Mapping[str, Any]) -> bool:
        """True when the same change was seen inside the window; records this one either way."""
        if not self.enabled:
            return False
        now = self.clock()
        key = event_identity(view)
        self._evict(now)
        last = self._seen.get(key)
        self._seen[key] = now
        self._seen.move_to_end(key)
        while len(self._seen) > MAX_TRACKED:
            self._seen.popitem(last=False)
        return last is not None and now - last < self.window

    def _evict(self, now: float) -> None:
        while self._seen:
            key, seen_at = next(iter(self._seen.items()))
            if now - seen_at < self.window:
                break
            self._seen.popitem(last=False)


class RuleRateLimiter:
    """Per-rule token bucket: ``max_per_minute`` fires per rolling minute, burst = the same."""

    def __init__(self, max_per_minute: Optional[int], clock: Callable[[], float] = time.monotonic):
        self.max_per_minute = int(max_per_minute) if max_per_minute else None
        self.clock = clock
        self._tokens: Dict[str, float] = {}
        self._updated: Dict[str, float] = {}

    @property
    def enabled(self) -> bool:
        return bool(self.max_per_minute and self.max_per_minute > 0)

    def allow(self, rule_id: str) -> bool:
        if not self.enabled:
            return True
        cap = float(self.max_per_minute)  # type: ignore[arg-type]
        now = self.clock()
        tokens = self._tokens.get(rule_id, cap)
        elapsed = now - self._updated.get(rule_id, now)
        tokens = min(cap, tokens + elapsed * cap / 60.0)
        self._updated[rule_id] = now
        if tokens < 1.0:
            self._tokens[rule_id] = tokens
            return False
        self._tokens[rule_id] = tokens - 1.0
        return True

    def describe(self) -> str:
        return f"{self.max_per_minute}/min per rule" if self.enabled else "no per-rule limit"


def limits_from_config(config: Mapping[str, Any]) -> tuple:
    """(RecentEvents, RuleRateLimiter) from a source config's `dedupeWindowSeconds` / `limits`."""
    window = config.get("dedupeWindowSeconds", DEFAULT_DEDUPE_WINDOW_SECONDS)
    limits = config.get("limits") or {}
    return RecentEvents(float(window if window is not None else 0)), RuleRateLimiter(limits.get("maxRunsPerRulePerMinute"))
