"""Spark Structured Streaming job: Kafka topic `reddit-raw` → Delta bronze.

Runs continuously (or in trigger=once mode for Airflow micro-batches).
Idempotency: Kafka offsets are checkpointed; downstream dedup happens
in the bronze→silver step.
"""

from __future__ import annotations

import click
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp

from pipeline.common.config import Settings
from pipeline.common.logging import configure_logging, get_logger
from pipeline.ingestion.schemas import bronze_post_schema

log = get_logger(__name__)


def parse_kafka_value(df: DataFrame) -> DataFrame:
    """Parse the `value` BINARY column from Kafka into typed columns."""
    schema = bronze_post_schema()
    return (
        df.select(col("value").cast("string").alias("json"))
        .select(from_json(col("json"), schema).alias("data"))
        .select("data.*")
        # Normalize ingest_ts: was ISO string from PRAW producer
        .withColumn("ingest_ts", to_timestamp(col("ingest_ts")))
    )


def build_spark(app_name: str) -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.jars.packages", _required_jars())
        .getOrCreate()
    )


def _required_jars() -> str:
    return ",".join(
        [
            "io.delta:delta-spark_2.12:3.2.0",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
            "org.apache.hadoop:hadoop-aws:3.3.4",
        ]
    )


def run_stream(
    *,
    spark: SparkSession,
    kafka_bootstrap: str,
    topic: str,
    bronze_path: str,
    checkpoint_path: str,
    trigger_once: bool = False,
) -> None:
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .load()
    )
    parsed = parse_kafka_value(raw)

    writer = (
        parsed.writeStream.format("delta")
        .option("checkpointLocation", checkpoint_path)
        .outputMode("append")
        .partitionBy("subreddit")
    )
    if trigger_once:
        writer = writer.trigger(availableNow=True)

    query = writer.start(bronze_path)
    query.awaitTermination()


@click.command()
@click.option("--trigger-once", is_flag=True, help="Run as a single batch (for Airflow)")
def main(trigger_once: bool) -> None:
    """Kafka → Bronze Delta stream."""
    configure_logging()
    settings = Settings()
    spark = build_spark("kafka-to-bronze")

    # MinIO/S3a config
    sc = spark.sparkContext
    hc = sc._jsc.hadoopConfiguration()
    hc.set("fs.s3a.access.key", settings.aws_access_key_id)
    hc.set("fs.s3a.secret.key", settings.aws_secret_access_key)
    if settings.s3_endpoint_url:
        hc.set("fs.s3a.endpoint", settings.s3_endpoint_url)
        hc.set("fs.s3a.path.style.access", "true")
    hc.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    bronze = f"s3a://{settings.s3_bronze_bucket}/posts/"
    checkpoint = f"s3a://{settings.s3_bronze_bucket}/_checkpoints/kafka_to_bronze/"

    log.info("starting_stream", bronze=bronze, topic=settings.kafka_topic_reddit_raw)
    run_stream(
        spark=spark,
        kafka_bootstrap=settings.kafka_bootstrap_servers,
        topic=settings.kafka_topic_reddit_raw,
        bronze_path=bronze,
        checkpoint_path=checkpoint,
        trigger_once=trigger_once,
    )


if __name__ == "__main__":
    main()
