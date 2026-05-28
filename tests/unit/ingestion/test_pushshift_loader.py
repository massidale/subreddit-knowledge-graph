from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from pipeline.ingestion.pushshift_loader import load_pushshift_dump

FIXTURES = Path(__file__).parent.parent.parent / "fixtures"


@pytest.fixture(scope="module")
def spark():
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("test-pushshift")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield spark
    spark.stop()


def test_load_pushshift_dump_parses_jsonl_gz(spark):
    path = str(FIXTURES / "sample_pushshift.jsonl.gz")
    df = load_pushshift_dump(
        spark=spark,
        input_path=path,
        ingest_source="pushshift",
        pipeline_version="0.1.0",
    )

    assert df.count() == 20
    cols = set(df.columns)
    assert {"id", "subreddit", "created_utc", "kind", "ingest_ts", "pipeline_version"}.issubset(cols)

    first = df.collect()[0]
    assert first.kind == "post"
    assert first.ingest_source == "pushshift"
    assert first.pipeline_version == "0.1.0"
