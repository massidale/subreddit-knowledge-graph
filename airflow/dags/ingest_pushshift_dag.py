"""Pushshift dump batch loader.

Manual trigger (triggered once per subreddit dump). Reads the gzipped
JSONL from S3 input prefix, writes Delta to bronze.
"""

from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.models.param import Param
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

with DAG(
    dag_id="ingest_pushshift_batch",
    description="One-shot loader for Pushshift dumps into bronze Delta",
    schedule=None,
    start_date=datetime(2026, 5, 27),
    catchup=False,
    params={
        "input_path": Param(
            "s3a://reddit-raw-dumps/PokemonPlatinum_2024.jsonl.gz",
            type="string",
            description="Path to the Pushshift dump file (s3a:// or local)",
        ),
        "output_path": Param(
            "s3a://bronze/posts/",
            type="string",
            description="Target bronze Delta path",
        ),
    },
    tags=["ingestion", "batch"],
) as dag:
    SparkSubmitOperator(
        task_id="load_pushshift",
        application="/opt/airflow/pipeline/ingestion/pushshift_loader.py",
        conn_id="spark_default",
        packages=(
            "io.delta:delta-spark_2.12:3.2.0,"
            "org.apache.hadoop:hadoop-aws:3.3.4"
        ),
        application_args=[
            "--input-path", "{{ params.input_path }}",
            "--output-path", "{{ params.output_path }}",
        ],
    )
