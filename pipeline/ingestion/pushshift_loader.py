"""Pushshift dump loader (Spark batch job).

Reads gzipped JSONL dumps of Reddit posts/comments and writes them to
the bronze Delta layer with full schema alignment.
"""

from __future__ import annotations

import click
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    current_timestamp,
    lit,
    when,
)

from pipeline.common.config import Settings
from pipeline.common.logging import configure_logging, get_logger
from pipeline.ingestion.schemas import bronze_post_schema

log = get_logger(__name__)


def load_pushshift_dump(
    *,
    spark: SparkSession,
    input_path: str,
    ingest_source: str,
    pipeline_version: str,
) -> DataFrame:
    """Read a Pushshift jsonl.gz dump and align it to the bronze schema."""
    raw = spark.read.json(input_path)

    # Ensure `body` and `title` columns exist before referencing them in `when`.
    # Pushshift fixtures containing only posts will not have a `body` column,
    # and fixtures containing only comments may not have a `title` column.
    # Adding nulls unconditionally prevents AnalysisException on missing columns.
    working = raw
    if "body" not in raw.columns:
        working = working.withColumn("body", lit(None).cast("string"))
    if "title" not in raw.columns:
        working = working.withColumn("title", lit(None).cast("string"))

    # Determine `kind`: Pushshift comments have `body`, posts have `selftext`/`title`.
    with_kind = working.withColumn(
        "kind",
        when(
            working["body"].isNotNull() & working["title"].isNull(), lit("comment")
        ).otherwise(lit("post")),
    )

    # Align to bronze schema: add missing columns as null
    schema = bronze_post_schema()
    field_names = {f.name for f in schema.fields}
    existing = set(with_kind.columns)
    for name in field_names - existing:
        # Special-cased fields added below
        if name in {"ingest_ts", "ingest_source", "pipeline_version"}:
            continue
        with_kind = with_kind.withColumn(name, lit(None))

    # Add ingestion metadata
    enriched = (
        with_kind.withColumn("ingest_ts", current_timestamp())
        .withColumn("ingest_source", lit(ingest_source))
        .withColumn("pipeline_version", lit(pipeline_version))
    )

    # Reorder & cast to schema
    select_exprs = [enriched[f.name].cast(f.dataType).alias(f.name) for f in schema.fields]
    return enriched.select(*select_exprs)


def build_spark(app_name: str) -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )


@click.command()
@click.option("--input-path", required=True, help="s3a:// or local path to jsonl.gz dump")
@click.option("--output-path", required=True, help="s3a:// bronze Delta target")
def main(input_path: str, output_path: str) -> None:
    """Pushshift batch loader CLI."""
    configure_logging()
    settings = Settings()
    spark = build_spark("pushshift-loader")

    df = load_pushshift_dump(
        spark=spark,
        input_path=input_path,
        ingest_source="pushshift",
        pipeline_version=settings.pipeline_version,
    )
    (df.write.format("delta").mode("append").partitionBy("subreddit").save(output_path))
    log.info("pushshift_load_complete", count=df.count(), output=output_path)


if __name__ == "__main__":
    main()
