"""Centralized configuration via environment variables."""

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment.

    Required env vars must be set (no defaults for secrets).
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Reddit
    reddit_client_id: str = Field(..., alias="REDDIT_CLIENT_ID")
    reddit_client_secret: str = Field(..., alias="REDDIT_CLIENT_SECRET")
    reddit_user_agent: str = Field(..., alias="REDDIT_USER_AGENT")

    # Kafka
    kafka_bootstrap_servers: str = Field(..., alias="KAFKA_BOOTSTRAP_SERVERS")
    kafka_topic_reddit_raw: str = Field("reddit-raw", alias="KAFKA_TOPIC_REDDIT_RAW")

    # S3 / MinIO
    s3_endpoint_url: str | None = Field(None, alias="S3_ENDPOINT_URL")
    s3_bronze_bucket: str = Field(..., alias="S3_BRONZE_BUCKET")
    s3_silver_bucket: str = Field("silver", alias="S3_SILVER_BUCKET")
    s3_gold_bucket: str = Field("gold", alias="S3_GOLD_BUCKET")
    aws_access_key_id: str = Field(..., alias="AWS_ACCESS_KEY_ID")
    aws_secret_access_key: str = Field(..., alias="AWS_SECRET_ACCESS_KEY")
    aws_region: str = Field("us-east-1", alias="AWS_REGION")

    # Pipeline metadata
    pipeline_version: str = Field(..., alias="PIPELINE_VERSION")
    pii_salt: str = Field("dev-salt-change-me", alias="PII_SALT")
