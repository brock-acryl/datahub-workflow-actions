"""§21 E4 — scheduled rules. ``Scheduler`` mirrors ``RecipeWatcher``: a timer thread whose unit
of work (``due_ticks``) is pure and testable with a fake clock. Each schedule rule keeps the
last tick it ran; on every check the ticks due since then are computed with croniter in the
rule's timezone. Without ``catchUp`` only the latest missed tick fires (an engine that was down
for a day runs once, not 24 times); with it every missed tick fires, oldest first, capped."""

from __future__ import annotations

import logging
import threading
import time
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Tuple

logger = logging.getLogger("datahub_workflow_actions.schedule")

MAX_CATCH_UP = 100
SCHEDULES_FLOW_ID = "schedules"


def validate_cron(expression: str) -> Optional[str]:
    try:
        from croniter import croniter
    except ImportError:  # pragma: no cover
        return "croniter is not installed"
    if not croniter.is_valid(expression):
        return f"invalid cron expression '{expression}'"
    return None


def validate_timezone(name: str) -> Optional[str]:
    try:
        from zoneinfo import ZoneInfo

        ZoneInfo(name)
    except Exception:  # noqa: BLE001
        return f"unknown timezone '{name}'"
    return None


def tick_id(rule_id: str, tick_ms: int) -> str:
    return f"schedule:{rule_id}:{tick_ms}"


def _zone(name: str):
    from zoneinfo import ZoneInfo

    return ZoneInfo(name or "UTC")


def ticks_between(cron: str, tz: str, after_ms: int, until_ms: int, limit: int = MAX_CATCH_UP) -> List[int]:
    """Cron fire times strictly after ``after_ms`` and at or before ``until_ms``, as epoch ms.
    Cron is evaluated in ``tz`` so DST shifts move the local fire time, not the wall clock."""
    from croniter import croniter

    zone = _zone(tz)
    start = datetime.fromtimestamp(after_ms / 1000, tz=zone)
    it = croniter(cron, start)
    out: List[int] = []
    while len(out) < limit:
        nxt: datetime = it.get_next(datetime)
        nxt_ms = int(nxt.timestamp() * 1000)
        if nxt_ms > until_ms:
            break
        out.append(nxt_ms)
    return out


def due_ticks(rule: Any, last_ms: int, now_ms: int) -> List[int]:
    """Ticks a schedule rule should run now: all missed ones (oldest first) with catchUp, else
    only the latest."""
    ticks = ticks_between(rule.on.cron, rule.on.timezone, last_ms, now_ms)
    if not ticks:
        return []
    return ticks if rule.on.catchUp else ticks[-1:]


def next_tick_ms(rule: Any, after_ms: int) -> Optional[int]:
    ticks = ticks_between(rule.on.cron, rule.on.timezone, after_ms, after_ms + 366 * 24 * 3600 * 1000, limit=1)
    return ticks[0] if ticks else None


class Scheduler:
    """Drives ``handle(rule, tick_ms)`` for every due tick of every schedule rule.

    ``rules`` is a callable so hot reload is free: each check reads the current rules; a rule
    that appears starts from *now* (nothing in the past is replayed), one that disappears is
    forgotten."""

    def __init__(
        self,
        rules: Callable[[], List[Any]],
        handle: Callable[[Any, int], Any],
        check_seconds: float = 15.0,
        clock: Callable[[], float] = time.time,
    ):
        self.rules = rules
        self.handle = handle
        self.check_seconds = max(1.0, float(check_seconds))
        self.clock = clock
        self.last_ms: Dict[str, int] = {}
        self.fired = 0
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def check_once(self) -> List[Tuple[str, int]]:
        """Runs every due tick; returns [(rule id, tick ms)] that fired."""
        now_ms = int(self.clock() * 1000)
        fired: List[Tuple[str, int]] = []
        current = {rule.id: rule for rule in self.rules() if getattr(rule, "enabled", True)}
        for gone in [rule_id for rule_id in self.last_ms if rule_id not in current]:
            del self.last_ms[gone]
        for rule_id, rule in current.items():
            if rule_id not in self.last_ms:
                self.last_ms[rule_id] = now_ms  # new (or newly enabled) rule: start counting from now
                continue
            for tick in due_ticks(rule, self.last_ms[rule_id], now_ms):
                try:
                    self.handle(rule, tick)
                except Exception as e:  # noqa: BLE001 — one bad tick must not stop the schedule
                    logger.warning("workflow-actions: schedule tick for %s failed: %s", rule_id, e)
                fired.append((rule_id, tick))
                self.fired += 1
            self.last_ms[rule_id] = now_ms
        return fired

    def describe(self) -> str:
        rules = list(self.rules())
        if not rules:
            return "no schedule rules"
        return ", ".join(f"{r.id} ({r.on.cron} {r.on.timezone})" for r in rules)

    def _loop(self) -> None:
        while not self._stop.wait(self.check_seconds):
            self.check_once()

    def start(self) -> None:
        if self._thread is not None:
            return
        self.check_once()  # registers the current rules' starting points
        self._thread = threading.Thread(target=self._loop, name="workflow-actions-schedule", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=5)
            self._thread = None


def iso(tick_ms: int, tz: str) -> str:
    return datetime.fromtimestamp(tick_ms / 1000, tz=_zone(tz)).isoformat()


def utc_iso(tick_ms: int) -> str:
    return datetime.fromtimestamp(tick_ms / 1000, tz=timezone.utc).isoformat()
