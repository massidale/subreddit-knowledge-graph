"""Periodic PRAW poller → Kafka.

Runs every 10 minutes. Idempotency comes from downstream dedup on
(id, subreddit) in bronze→silver.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

DEFAULT_ARGS = {
    "owner": "team",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="ingest_praw_streaming",
    description="Poll Reddit API via PRAW and publish to Kafka topic",
    default_args=DEFAULT_ARGS,
    schedule="*/10 * * * *",
    start_date=datetime(2026, 5, 27),
    catchup=False,
    max_active_runs=1,
    tags=["ingestion", "streaming"],
) as dag:
    for subreddit in ["PokemonPlatinum", "chess"]:
        BashOperator(
            task_id=f"praw_fetch_{subreddit.lower()}",
            bash_command=(
                "cd /opt/airflow && "
                f"python -m pipeline.ingestion.cli praw-fetch "
                f"--subreddit {subreddit} --limit 100"
            ),
            env={
                "PYTHONPATH": "/opt/airflow",
                # Airflow runtime should mount .env or define vars in compose
            },
        )
