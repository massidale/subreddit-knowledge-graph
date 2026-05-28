"""One-shot fixture generator. Run once with:
    uv run python tests/fixtures/_generate_pushshift_fixture.py
"""

import gzip
import json
from pathlib import Path


def main():
    records = [
        {
            "id": f"pid{i:03d}",
            "subreddit": "PokemonPlatinum",
            "author": f"user_{i}",
            "title": f"Title {i}",
            "selftext": f"Discussion about Pokemon strategy number {i}.",
            "created_utc": 1700000000 + i * 60,
            "score": i * 3,
            "is_self": True,
            "permalink": f"/r/PokemonPlatinum/x{i}/",
            "url": f"https://reddit.com/x{i}",
        }
        for i in range(20)
    ]
    out = Path(__file__).parent / "sample_pushshift.jsonl.gz"
    with gzip.open(out, "wt", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r) + "\n")
    print(f"Wrote {len(records)} records to {out}")


if __name__ == "__main__":
    main()
