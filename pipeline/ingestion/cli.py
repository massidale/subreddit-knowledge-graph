"""CLI entry points for ingestion jobs."""

from __future__ import annotations

import click
import praw
from kafka import KafkaProducer

from pipeline.common.config import Settings
from pipeline.common.logging import configure_logging, get_logger
from pipeline.ingestion.praw_producer import PrawProducer

log = get_logger(__name__)


@click.group()
def cli() -> None:
    """Ingestion CLI."""
    configure_logging()


@cli.command("praw-fetch")
@click.option("--subreddit", required=True, help="Subreddit name (without r/)")
@click.option("--limit", default=100, type=int, help="Max items per run")
def praw_fetch(subreddit: str, limit: int) -> None:
    """Poll PRAW and publish to Kafka topic `reddit-raw`."""
    settings = Settings()
    reddit = praw.Reddit(
        client_id=settings.reddit_client_id,
        client_secret=settings.reddit_client_secret,
        user_agent=settings.reddit_user_agent,
    )
    producer = KafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        acks="all",
        linger_ms=200,
        compression_type="gzip",
    )
    pp = PrawProducer(
        reddit=reddit,
        kafka_producer=producer,
        topic=settings.kafka_topic_reddit_raw,
        pipeline_version=settings.pipeline_version,
    )
    n = pp.fetch_and_publish(subreddit=subreddit, limit=limit)
    click.echo(f"Published {n} items to {settings.kafka_topic_reddit_raw}")


if __name__ == "__main__":
    cli()
