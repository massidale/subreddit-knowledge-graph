import json

import pytest
from pyspark.sql import SparkSession

from pipeline.ingestion.kafka_to_bronze import parse_kafka_value


@pytest.fixture(scope="module")
def spark():
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("test-kafka-to-bronze")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield spark
    spark.stop()


def test_parse_kafka_value_returns_typed_columns(spark):
    payload = {
        "id": "abc123",
        "subreddit": "PokemonPlatinum",
        "author": "trainer_red",
        "title": "Title",
        "selftext": "body",
        "body": None,
        "created_utc": 1716800000,
        "score": 42,
        "is_self": True,
        "parent_id": None,
        "permalink": "/r/PokemonPlatinum/x/",
        "url": "https://reddit.com/x",
        "kind": "post",
        "ingest_ts": "2026-05-27T10:00:00+00:00",
        "ingest_source": "praw",
        "pipeline_version": "0.1.0",
    }
    raw = [(json.dumps(payload).encode("utf-8"),)]
    df = spark.createDataFrame(raw, "value BINARY")

    parsed = parse_kafka_value(df)
    row = parsed.collect()[0]

    assert row.id == "abc123"
    assert row.created_utc == 1716800000
    assert row.score == 42
    assert row.is_self is True
    assert row.kind == "post"
    assert row.pipeline_version == "0.1.0"
