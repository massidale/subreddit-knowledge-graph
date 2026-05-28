import pytest
from pydantic import ValidationError

from pipeline.common.config import Settings


def test_settings_loads_from_env(monkeypatch):
    monkeypatch.setenv("REDDIT_CLIENT_ID", "abc")
    monkeypatch.setenv("REDDIT_CLIENT_SECRET", "xyz")
    monkeypatch.setenv("REDDIT_USER_AGENT", "test-bot/0.1 by u/test")
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    monkeypatch.setenv("S3_ENDPOINT_URL", "http://minio:9000")
    monkeypatch.setenv("S3_BRONZE_BUCKET", "bronze")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "minioadmin")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "minioadmin")
    monkeypatch.setenv("PIPELINE_VERSION", "0.1.0")

    s = Settings()

    assert s.reddit_client_id == "abc"
    assert s.kafka_bootstrap_servers == "localhost:9092"
    assert s.s3_bronze_bucket == "bronze"
    assert s.pipeline_version == "0.1.0"


def test_settings_missing_required_raises(monkeypatch):
    monkeypatch.delenv("REDDIT_CLIENT_ID", raising=False)
    # Disable .env file loading so the test isn't masked by a local .env
    with pytest.raises(ValidationError):
        Settings(_env_file=None)
