"""S3/MinIO storage helpers.

Wraps boto3 to provide a unified interface that works against both
real S3 and a local MinIO instance (transparent to callers).
"""

from __future__ import annotations

import boto3
from botocore.client import Config

from pipeline.common.config import Settings


def get_s3_client(settings: Settings):
    """Return an S3 client configured for MinIO or AWS S3."""
    kwargs = {
        "service_name": "s3",
        "aws_access_key_id": settings.aws_access_key_id,
        "aws_secret_access_key": settings.aws_secret_access_key,
        "region_name": settings.aws_region,
        "config": Config(signature_version="s3v4"),
    }
    if settings.s3_endpoint_url:
        kwargs["endpoint_url"] = settings.s3_endpoint_url
    return boto3.client(**kwargs)


def s3a_path(bucket: str, prefix: str = "") -> str:
    """Return an `s3a://` path for use with Spark."""
    prefix = prefix.lstrip("/")
    return f"s3a://{bucket}/{prefix}" if prefix else f"s3a://{bucket}"
