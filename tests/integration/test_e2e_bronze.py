"""End-to-end integration test for the bronze ingestion path.

Spins up Kafka via testcontainers, publishes synthetic events through the
PrawProducer interface (with a fake PRAW), runs a single batch of the
Kafka→Bronze Spark job (trigger=availableNow), and asserts that bronze
Delta tables contain the expected records.

Note: MinioContainer was removed from the original spec because the `minio`
Python SDK is not installed in the dev dependencies (testcontainers[kafka]
only). The Delta assertions use local tmp_path, so MinIO is not needed.
"""

import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from kafka import KafkaProducer
from testcontainers.kafka import KafkaContainer

from pipeline.ingestion.kafka_to_bronze import build_spark, run_stream
from pipeline.ingestion.praw_producer import PrawProducer

FIXTURE = Path(__file__).parent.parent / "fixtures" / "sample_praw_post.json"


@pytest.mark.integration
@pytest.mark.slow
def test_kafka_to_bronze_end_to_end(tmp_path):
    with KafkaContainer() as kafka:
        bootstrap = kafka.get_bootstrap_server()

        # 1. Publish a fake post via PrawProducer
        producer = KafkaProducer(bootstrap_servers=bootstrap)
        fake_reddit = MagicMock()
        fake_sub = MagicMock()
        data = json.loads(FIXTURE.read_text())
        # Apply non-author fields first; skip "author" since it's a string in
        # the fixture but PrawProducer expects an object with a .name attribute.
        for k, v in data.items():
            if k != "author":
                setattr(fake_sub, k, v)
        fake_sub.kind = "t3"
        fake_sub.subreddit = "PokemonPlatinum"
        fake_sub.author = MagicMock()
        fake_sub.author.name = "trainer_red"
        fake_reddit.subreddit.return_value.new.return_value = iter([fake_sub])

        pp = PrawProducer(
            reddit=fake_reddit,
            kafka_producer=producer,
            topic="reddit-raw",
            pipeline_version="0.1.0",
        )
        assert pp.fetch_and_publish(subreddit="PokemonPlatinum", limit=1) == 1
        producer.flush()
        producer.close()

        # 2. Run the Kafka→Bronze stream in trigger=availableNow mode
        bronze_dir = str(tmp_path / "bronze")
        ckpt_dir = str(tmp_path / "ckpt")
        spark = build_spark("test-e2e")
        try:
            run_stream(
                spark=spark,
                kafka_bootstrap=bootstrap,
                topic="reddit-raw",
                bronze_path=bronze_dir,
                checkpoint_path=ckpt_dir,
                trigger_once=True,
            )

            # 3. Assert the bronze Delta has the row
            df = spark.read.format("delta").load(bronze_dir)
            rows = df.collect()
            assert len(rows) == 1
            assert rows[0].id == "abc123"
            assert rows[0].subreddit == "PokemonPlatinum"
            assert rows[0].kind == "post"
        finally:
            spark.stop()
