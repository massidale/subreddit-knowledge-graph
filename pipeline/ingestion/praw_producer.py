"""PRAW producer: poll Reddit API and publish to Kafka.

Designed to be invoked periodically by Airflow (e.g. every 5 minutes).
Idempotency: Kafka topic acts as the immutable log; downstream Spark
Structured Streaming deduplicates on (id, subreddit).
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

import praw
from kafka import KafkaProducer

from pipeline.common.logging import get_logger

log = get_logger(__name__)


def serialize_submission(
    sub: Any,
    *,
    ingest_source: str,
    pipeline_version: str,
) -> dict[str, Any]:
    """Map a PRAW Submission/Comment object to the bronze schema shape.

    `kind` is normalized to 'post' or 'comment'. PRAW internal kinds
    (`t1`=comment, `t3`=post) are mapped here.
    """
    raw_kind = getattr(sub, "kind", "t3")
    kind = "comment" if raw_kind == "t1" else "post"

    # `body` and `parent_id` are comment-only fields; posts use `selftext`.
    body = getattr(sub, "body", None) if kind == "comment" else None
    parent_id = getattr(sub, "parent_id", None) if kind == "comment" else None

    return {
        "id": sub.id,
        "subreddit": str(sub.subreddit),
        "author": getattr(sub.author, "name", None) if sub.author else None,
        "title": getattr(sub, "title", None),
        "selftext": getattr(sub, "selftext", None),
        "body": body,
        "created_utc": int(sub.created_utc),
        "score": getattr(sub, "score", None),
        "is_self": getattr(sub, "is_self", None),
        "parent_id": parent_id,
        "permalink": getattr(sub, "permalink", None),
        "url": getattr(sub, "url", None),
        "kind": kind,
        "ingest_ts": datetime.now(UTC).isoformat(),
        "ingest_source": ingest_source,
        "pipeline_version": pipeline_version,
    }


class PrawProducer:
    """Fetch new submissions/comments from a subreddit and publish to Kafka."""

    def __init__(
        self,
        *,
        reddit: praw.Reddit,
        kafka_producer: KafkaProducer,
        topic: str,
        pipeline_version: str,
    ) -> None:
        self._reddit = reddit
        self._kafka = kafka_producer
        self._topic = topic
        self._pipeline_version = pipeline_version

    def fetch_and_publish(self, *, subreddit: str, limit: int = 100) -> int:
        """Fetch up to `limit` newest items from `subreddit` and publish."""
        sub = self._reddit.subreddit(subreddit)
        count = 0
        for item in sub.new(limit=limit):
            payload = serialize_submission(
                item,
                ingest_source="praw",
                pipeline_version=self._pipeline_version,
            )
            self._kafka.send(
                self._topic,
                key=payload["id"].encode("utf-8"),
                value=json.dumps(payload).encode("utf-8"),
            )
            count += 1
        self._kafka.flush()
        log.info("praw_publish_complete", subreddit=subreddit, count=count)
        return count
