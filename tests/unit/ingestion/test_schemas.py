from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    LongType,
    StringType,
)

from pipeline.ingestion.schemas import bronze_post_schema, ingestion_metadata_fields


def test_bronze_schema_has_core_reddit_fields():
    schema = bronze_post_schema()
    names = {f.name for f in schema.fields}
    expected = {
        "id",
        "subreddit",
        "author",
        "title",
        "selftext",
        "body",
        "created_utc",
        "score",
        "is_self",
        "parent_id",
        "permalink",
        "url",
        "kind",  # 'post' or 'comment'
    }
    assert expected.issubset(names)


def test_bronze_schema_has_ingestion_metadata():
    schema = bronze_post_schema()
    names = {f.name for f in schema.fields}
    assert "ingest_ts" in names
    assert "ingest_source" in names
    assert "pipeline_version" in names


def test_created_utc_is_long():
    schema = bronze_post_schema()
    f = next(f for f in schema.fields if f.name == "created_utc")
    assert isinstance(f.dataType, LongType)


def test_score_is_integer():
    schema = bronze_post_schema()
    f = next(f for f in schema.fields if f.name == "score")
    assert isinstance(f.dataType, IntegerType)


def test_is_self_is_boolean():
    schema = bronze_post_schema()
    f = next(f for f in schema.fields if f.name == "is_self")
    assert isinstance(f.dataType, BooleanType)


def test_ingestion_metadata_fields_returns_three():
    fields = ingestion_metadata_fields()
    assert len(fields) == 3
    assert all(isinstance(f.dataType, StringType) for f in fields if f.name != "ingest_ts")

def test_over18_is_boolean():
    schema = bronze_post_schema()
    f = next(f for f in schema.fields if f.name == "over_18")
    assert isinstance(f.dataType, BooleanType)
