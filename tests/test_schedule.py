from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from datahub_workflow_actions.contract import load_rules
from datahub_workflow_actions.schedule import Scheduler, due_ticks, next_tick_ms, tick_id, ticks_between, validate_cron, validate_timezone


def ms(iso: str) -> int:
    return int(datetime.fromisoformat(iso).timestamp() * 1000)


def rule(rid="nightly", cron="0 6 * * *", tz="UTC", catch_up=False, enabled=True):
    return load_rules({"schemaVersion": 1, "rules": [{"id": rid, "enabled": enabled, "on": {"type": "schedule", "cron": cron, "timezone": tz, "catchUp": catch_up}, "steps": []}]}).rules[0]


def test_validators():
    assert validate_cron("0 6 * * 1-5") is None and validate_cron("nope") and validate_cron("* * * *")
    assert validate_timezone("UTC") is None and validate_timezone("America/New_York") is None and validate_timezone("Nowhere/City")


def test_ticks_between_is_exclusive_of_the_start_and_inclusive_of_the_end():
    start = ms("2026-03-01T05:00:00+00:00")
    assert ticks_between("0 6 * * *", "UTC", start, ms("2026-03-01T06:00:00+00:00")) == [ms("2026-03-01T06:00:00+00:00")]
    assert ticks_between("0 6 * * *", "UTC", ms("2026-03-01T06:00:00+00:00"), ms("2026-03-01T07:00:00+00:00")) == []
    assert len(ticks_between("*/10 * * * *", "UTC", start, ms("2026-03-01T06:00:00+00:00"))) == 6
    assert len(ticks_between("* * * * *", "UTC", start, start + 10 * 24 * 3600 * 1000)) == 100  # capped


def test_cron_is_read_in_the_rule_timezone_across_dst():
    # 06:00 Berlin is 05:00 UTC in winter and 04:00 UTC in summer; the DST switch is 2026-03-29.
    berlin = "Europe/Berlin"
    before = ticks_between("0 6 * * *", berlin, ms("2026-03-27T00:00:00+00:00"), ms("2026-03-28T23:59:00+00:00"))
    after = ticks_between("0 6 * * *", berlin, ms("2026-03-30T00:00:00+00:00"), ms("2026-03-31T23:59:00+00:00"))
    assert [datetime.fromtimestamp(t / 1000, tz=timezone.utc).hour for t in before] == [5, 5]
    assert [datetime.fromtimestamp(t / 1000, tz=timezone.utc).hour for t in after] == [4, 4]
    assert all(datetime.fromtimestamp(t / 1000, tz=ZoneInfo(berlin)).hour == 6 for t in before + after)


def test_missed_ticks_latest_only_unless_catch_up():
    last = ms("2026-03-01T00:00:00+00:00")
    now = ms("2026-03-04T12:00:00+00:00")  # three nightly ticks missed
    assert due_ticks(rule(), last, now) == [ms("2026-03-04T06:00:00+00:00")]
    assert due_ticks(rule(catch_up=True), last, now) == [ms(f"2026-03-0{d}T06:00:00+00:00") for d in (1, 2, 3, 4)]
    assert due_ticks(rule(), now, now + 1000) == []
    assert next_tick_ms(rule(), now) == ms("2026-03-05T06:00:00+00:00")
    assert tick_id("nightly", 5) == "schedule:nightly:5"


class Clock:
    def __init__(self, start_iso):
        self.now = datetime.fromisoformat(start_iso).timestamp()

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += seconds


def test_scheduler_fires_due_ticks_and_follows_rule_changes():
    clock = Clock("2026-03-01T05:59:00+00:00")
    rules = [rule("nightly"), rule("often", cron="*/1 * * * *")]
    fired = []
    scheduler = Scheduler(rules=lambda: rules, handle=lambda r, t: fired.append((r.id, t)), check_seconds=15, clock=clock)
    assert scheduler.check_once() == []  # first check only registers starting points — nothing from the past
    clock.advance(61)
    assert sorted(scheduler.check_once()) == [("nightly", ms("2026-03-01T06:00:00+00:00")), ("often", ms("2026-03-01T06:00:00+00:00"))]
    assert len(fired) == 2
    # the engine was busy for 5 minutes: 'often' ran once (latest only), 'nightly' not at all
    clock.advance(5 * 60)
    assert scheduler.check_once() == [("often", ms("2026-03-01T06:05:00+00:00"))]
    # hot reload: a new rule starts from now, a removed rule is forgotten, a disabled one is skipped
    rules[:] = [rule("nightly", enabled=False), rule("fresh", cron="*/1 * * * *")]
    assert scheduler.check_once() == []
    assert set(scheduler.last_ms) == {"fresh"}
    clock.advance(60)
    assert scheduler.check_once() == [("fresh", ms("2026-03-01T06:06:00+00:00"))]
    # a failing handler is logged, never raised, and the tick still counts as consumed
    broken = Scheduler(rules=lambda: [rule("boom", cron="* * * * *")], handle=lambda r, t: 1 / 0, check_seconds=15, clock=clock)
    broken.check_once()
    clock.advance(60)
    assert len(broken.check_once()) == 1 and broken.fired == 1
    assert "boom (* * * * *" in broken.describe()
