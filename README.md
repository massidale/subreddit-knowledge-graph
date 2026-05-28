# Reddit → Community Discourse KG Pipeline

Pipeline big-data subreddit-agnostic per costruire un Knowledge Graph in Neo4j a partire da un subreddit, focus su discorso comunitario (claim reificate con sentiment/stance).

## Stack

Python 3.11 · Apache Spark 3.5 · Delta Lake · Apache Kafka · Apache Airflow · AWS (S3, EMR) · Neo4j 5.x · HuggingFace · Claude API.

## Setup

Vedi `docs/superpowers/plans/2026-05-27-reddit-kg-pipeline-implementation.md`.

### Quickstart locale

```bash
# Install uv (https://docs.astral.sh/uv/)
curl -LsSf https://astral.sh/uv/install.sh | sh

# Setup project
uv sync

# Start local dev stack
cd docker && docker compose up -d

# Run tests
uv run pytest
```

## Documentazione

- Spec: `docs/superpowers/specs/2026-05-27-reddit-kg-pipeline-design.md`
- Plan: `docs/superpowers/plans/2026-05-27-reddit-kg-pipeline-implementation.md`
