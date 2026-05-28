import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from pipeline.ingestion.praw_producer import (
    PrawProducer,
    serialize_submission,
)

FIXTURES = Path(__file__).parent.parent.parent / "fixtures"


@pytest.fixture
def sample_submission_dict() -> dict:
    return json.loads((FIXTURES / "sample_praw_post.json").read_text())


@pytest.fixture
def fake_praw_submission(sample_submission_dict):
    sub = MagicMock()
    for k, v in sample_submission_dict.items():
        setattr(sub, k, v)
    sub.kind = "t3"  # PRAW internal — we'll normalize to 'post'
    return sub


def test_serialize_submission_maps_post_fields(fake_praw_submission):
    out = serialize_submission(fake_praw_submission, ingest_source="praw", pipeline_version="0.1.0")

    assert out["id"] == "abc123"
    assert out["subreddit"] == "PokemonPlatinum"
    assert out["kind"] == "post"
    assert out["title"] == "Best moveset for Garchomp?"
    assert out["selftext"].startswith("I just caught")
    assert out["body"] is None
    assert out["created_utc"] == 1716800000
    assert out["score"] == 42
    assert out["is_self"] is True
    assert out["ingest_source"] == "praw"
    assert out["pipeline_version"] == "0.1.0"
    assert isinstance(out["ingest_ts"], str)  # ISO


def test_producer_publishes_to_kafka(fake_praw_submission, mocker):
    mock_kafka = mocker.MagicMock()
    mock_reddit = mocker.MagicMock()
    mock_reddit.subreddit.return_value.new.return_value = iter([fake_praw_submission])

    p = PrawProducer(
        reddit=mock_reddit,
        kafka_producer=mock_kafka,
        topic="reddit-raw",
        pipeline_version="0.1.0",
    )
    n = p.fetch_and_publish(subreddit="PokemonPlatinum", limit=1)

    assert n == 1
    mock_kafka.send.assert_called_once()
    args, kwargs = mock_kafka.send.call_args
    assert args[0] == "reddit-raw"
    payload = json.loads(kwargs["value"].decode("utf-8"))
    assert payload["id"] == "abc123"
    assert payload["kind"] == "post"
