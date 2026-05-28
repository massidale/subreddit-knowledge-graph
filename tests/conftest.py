"""Shared pytest fixtures."""

import pytest


@pytest.fixture(autouse=True)
def _isolate_env(monkeypatch):
    """Ensure tests don't accidentally inherit user env vars."""
    for var in [
        "REDDIT_CLIENT_ID",
        "REDDIT_CLIENT_SECRET",
        "REDDIT_USER_AGENT",
        "KAFKA_BOOTSTRAP_SERVERS",
        "S3_ENDPOINT_URL",
        "S3_BRONZE_BUCKET",
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "PIPELINE_VERSION",
    ]:
        monkeypatch.delenv(var, raising=False)
