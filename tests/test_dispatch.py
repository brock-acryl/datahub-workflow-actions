from datahub_workflow_actions.dispatch import RecentEvents, RuleRateLimiter, event_identity, limits_from_config
from tests.conftest import DATASET, TAG_PII, tag_added_event


class Clock:
    def __init__(self):
        self.now = 1000.0

    def __call__(self):
        return self.now


def view(**over):
    e = tag_added_event(**over)
    return {"entityUrn": e["entityUrn"], "category": e["category"], "operation": e["operation"], "modifier": e["modifier"]}


def test_event_identity_ignores_time_and_actor():
    assert event_identity(view()) == f"{DATASET}:TAG:ADD:{TAG_PII}"
    assert event_identity(view(time=1, actor="urn:li:corpuser:x")) == event_identity(view())
    assert event_identity(view(tag="urn:li:tag:other")) != event_identity(view())
    assert event_identity({"entityUrn": DATASET, "category": "DEPRECATION", "operation": "MODIFY", "modifier": None}) == f"{DATASET}:DEPRECATION:MODIFY:"


def test_recent_events_window():
    clock = Clock()
    recent = RecentEvents(30, clock)
    assert recent.duplicate(view()) is False  # first sighting
    clock.now += 10
    assert recent.duplicate(view()) is True  # twin event (schemaMetadata vs editableSchemaMetadata)
    assert recent.duplicate(view(tag="urn:li:tag:other")) is False  # a different change is not a duplicate
    clock.now += 31
    assert recent.duplicate(view()) is False  # the window has passed since the last sighting
    # a disabled window never reports duplicates
    off = RecentEvents(0, clock)
    assert off.enabled is False and off.duplicate(view()) is False and off.duplicate(view()) is False


def test_recent_events_bounded_memory():
    clock = Clock()
    recent = RecentEvents(3600, clock)
    for i in range(10500):
        recent.duplicate(view(entity_urn=f"urn:li:dataset:(x,{i},PROD)"))
    assert len(recent._seen) <= 10000


def test_rule_rate_limiter_token_bucket():
    clock = Clock()
    limiter = RuleRateLimiter(3, clock)
    assert [limiter.allow("r") for _ in range(4)] == [True, True, True, False]
    assert limiter.allow("other") is True  # buckets are per rule
    clock.now += 20  # a third of a minute refills one token
    assert limiter.allow("r") is True and limiter.allow("r") is False
    clock.now += 120  # never above the cap
    assert [limiter.allow("r") for _ in range(4)] == [True, True, True, False]
    assert limiter.describe() == "3/min per rule"
    assert RuleRateLimiter(None).enabled is False and RuleRateLimiter(None).allow("r") is True
    assert RuleRateLimiter(0).enabled is False


def test_limits_from_config_defaults_and_overrides():
    recent, limiter = limits_from_config({})
    assert recent.window == 30 and limiter.enabled is False
    recent, limiter = limits_from_config({"dedupeWindowSeconds": 0, "limits": {"maxRunsPerRulePerMinute": 5}})
    assert recent.enabled is False and limiter.max_per_minute == 5
