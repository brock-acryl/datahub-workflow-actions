"""The in-executor source must reach the broker the environment points at (§19 deploy)."""

from datahub_workflow_actions.source import (
    MAX_POLL_INTERVAL_MS,
    WorkflowActionsSource,
    WorkflowActionsSourceConfig,
    default_kafka_config,
    ensure_visible_logging,
    graph_config_from_env,
    with_consumer_defaults,
)


def test_kafka_defaults_come_from_the_executor_environment(monkeypatch):
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVER", "broker:29092")
    monkeypatch.setenv("SCHEMA_REGISTRY_URL", "http://datahub-gms:8080/schema-registry/api/")
    monkeypatch.setenv("KAFKA_PROPERTIES_SECURITY_PROTOCOL", "PLAINTEXT")
    assert default_kafka_config() == {
        "connection": {
            "bootstrap": "broker:29092",
            "schema_registry_url": "http://datahub-gms:8080/schema-registry/api/",
            "consumer_config": {"max.poll.interval.ms": MAX_POLL_INTERVAL_MS, "security.protocol": "PLAINTEXT"},
        }
    }


def test_kafka_defaults_fall_back_to_localhost(monkeypatch):
    for key in ("KAFKA_BOOTSTRAP_SERVER", "SCHEMA_REGISTRY_URL", "KAFKA_PROPERTIES_SECURITY_PROTOCOL"):
        monkeypatch.delenv(key, raising=False)
    assert default_kafka_config() == {
        "connection": {
            "bootstrap": "localhost:9092",
            "schema_registry_url": "http://localhost:8081",
            "consumer_config": {"max.poll.interval.ms": MAX_POLL_INTERVAL_MS},
        }
    }


RULES = {"schemaVersion": 1, "rules": []}


class _Cfg:
    def __init__(self, **kw):
        self.__dict__.update(kw)


class _Graph:
    def __init__(self, **kw):
        self.config = _Cfg(**kw)


class _Ctx:
    def __init__(self, graph=None):
        self.graph = graph


def test_pipeline_uses_the_ingestion_graph_connection():
    ctx = _Ctx(_Graph(server="http://datahub-gms:8080", token="tok", timeout_sec=30, extra_headers={}, retry_max_times=None))
    src = WorkflowActionsSource(WorkflowActionsSourceConfig.model_validate(RULES), ctx)
    assert src.actions_pipeline_config()["datahub"] == {"server": "http://datahub-gms:8080", "token": "tok", "timeout_sec": 30}


def test_pipeline_falls_back_to_executor_env_for_the_connection(monkeypatch):
    for key in ("DATAHUB_GMS_URL", "DATAHUB_GMS_TOKEN"):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("DATAHUB_GMS_HOST", "datahub-gms")
    monkeypatch.setenv("DATAHUB_GMS_PORT", "8080")
    monkeypatch.setenv("DATAHUB_GMS_PROTOCOL", "http")
    assert graph_config_from_env() == {"server": "http://datahub-gms:8080"}
    monkeypatch.setenv("DATAHUB_GMS_URL", "https://acme.acryl.io/gms")
    monkeypatch.setenv("DATAHUB_GMS_TOKEN", "t")
    assert graph_config_from_env() == {"server": "https://acme.acryl.io/gms", "token": "t"}
    src = WorkflowActionsSource(WorkflowActionsSourceConfig.model_validate(RULES), _Ctx(None))
    assert src.actions_pipeline_config()["datahub"]["server"] == "https://acme.acryl.io/gms"


def test_pipeline_omits_datahub_when_nothing_is_known(monkeypatch):
    for key in ("DATAHUB_GMS_URL", "DATAHUB_GMS_HOST", "DATAHUB_GMS_TOKEN"):
        monkeypatch.delenv(key, raising=False)
    src = WorkflowActionsSource(WorkflowActionsSourceConfig.model_validate(RULES), _Ctx(None))
    assert "datahub" not in src.actions_pipeline_config()


def test_visible_logging_is_idempotent():
    import logging

    ensure_visible_logging()
    ensure_visible_logging()
    ours = logging.getLogger("datahub_workflow_actions")
    assert len([h for h in ours.handlers if getattr(h, "_workflow_actions", False)]) == 1
    assert ours.propagate is False and ours.level == logging.INFO


def test_poll_interval_is_raised_but_a_recipe_override_wins():
    assert int(MAX_POLL_INTERVAL_MS) == 15 * 60 * 1000
    merged = with_consumer_defaults({"connection": {"bootstrap": "k:9092", "consumer_config": {"max.poll.interval.ms": "60000", "group.id": "x"}}})
    assert merged["connection"]["consumer_config"]["max.poll.interval.ms"] == "60000"
    assert merged["connection"]["consumer_config"]["group.id"] == "x"
    assert merged["connection"]["bootstrap"] == "k:9092"


def test_consumer_group_is_per_executor(monkeypatch):
    monkeypatch.delenv("DATAHUB_EXECUTOR_WORKER_ID", raising=False)
    monkeypatch.delenv("DATAHUB_EXECUTOR_ID", raising=False)
    src = WorkflowActionsSource(WorkflowActionsSourceConfig.model_validate({**RULES, "executorId": "pool-eu"}), _Ctx(None))
    assert src.effective_pipeline_name() == "workflow-actions-pool-eu"
    assert src.actions_pipeline_config()["name"] == "workflow-actions-pool-eu"
    monkeypatch.setenv("DATAHUB_EXECUTOR_WORKER_ID", "remote-7")
    src = WorkflowActionsSource(WorkflowActionsSourceConfig.model_validate(RULES), _Ctx(None))
    assert src.effective_pipeline_name() == "workflow-actions-remote-7"
    src = WorkflowActionsSource(WorkflowActionsSourceConfig.model_validate({**RULES, "pipelineName": "custom"}), _Ctx(None))
    assert src.effective_pipeline_name() == "custom"
