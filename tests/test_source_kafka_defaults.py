"""The in-executor source must reach the broker the environment points at (§19 deploy)."""

from datahub_workflow_actions.source import default_kafka_config


def test_kafka_defaults_come_from_the_executor_environment(monkeypatch):
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVER", "broker:29092")
    monkeypatch.setenv("SCHEMA_REGISTRY_URL", "http://datahub-gms:8080/schema-registry/api/")
    monkeypatch.setenv("KAFKA_PROPERTIES_SECURITY_PROTOCOL", "PLAINTEXT")
    assert default_kafka_config() == {
        "connection": {
            "bootstrap": "broker:29092",
            "schema_registry_url": "http://datahub-gms:8080/schema-registry/api/",
            "consumer_config": {"security.protocol": "PLAINTEXT"},
        }
    }


def test_kafka_defaults_fall_back_to_localhost(monkeypatch):
    for key in ("KAFKA_BOOTSTRAP_SERVER", "SCHEMA_REGISTRY_URL", "KAFKA_PROPERTIES_SECURITY_PROTOCOL"):
        monkeypatch.delenv(key, raising=False)
    assert default_kafka_config() == {"connection": {"bootstrap": "localhost:9092", "schema_registry_url": "http://localhost:8081"}}
