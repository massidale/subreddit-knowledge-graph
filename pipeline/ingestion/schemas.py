"""PySpark schemas for the bronze layer.

The bronze schema accepts both Reddit posts and comments in a single
unified shape — the `kind` column distinguishes them. Some fields are
null for one or the other (e.g. `title` is null for comments).
"""

from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


def ingestion_metadata_fields() -> list[StructField]:
    """Metadata fields appended to every bronze record at ingestion time."""
    return [
        StructField("ingest_ts", TimestampType(), nullable=False),
        StructField("ingest_source", StringType(), nullable=False),  # 'praw' | 'pushshift'
        StructField("pipeline_version", StringType(), nullable=False),
    ]


def bronze_post_schema() -> StructType:
    """Unified bronze schema for Reddit posts AND comments."""
    return StructType(
        [
            StructField("id", StringType(), nullable=False),
            StructField("subreddit", StringType(), nullable=False),
            StructField("author", StringType(), nullable=True),
            StructField("title", StringType(), nullable=True),
            StructField("selftext", StringType(), nullable=True),
            StructField("body", StringType(), nullable=True),
            StructField("created_utc", LongType(), nullable=False),
            StructField("score", IntegerType(), nullable=True),
            StructField("is_self", BooleanType(), nullable=True),
            StructField("parent_id", StringType(), nullable=True),
            StructField("permalink", StringType(), nullable=True),
            StructField("url", StringType(), nullable=True),
            StructField("kind", StringType(), nullable=False),  # 'post' | 'comment'
            *ingestion_metadata_fields(),
        ]
    )
