# Reddit → Community Discourse KG Pipeline — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implementare la pipeline subreddit-agnostic descritta nella spec del 2026-05-27, producendo un Community Discourse Knowledge Graph in Neo4j a partire da un subreddit qualsiasi, con primo dominio di test r/PokemonPlatinum.

**Architecture:** Medallion Lakehouse (S3 + Delta Lake bronze/silver/gold) con ingestion ibrida (Pushshift batch + PRAW streaming via Kafka), processing Spark su EMR, orchestrazione Airflow. KG a due fasi (schema discovery LLM agnostico + teacher-student extraction) con claim reificate, sentiment e stance. Anchoring universale a Wikidata. Pipeline immagini con CLIP embedding-only.

**Tech Stack:** Python 3.11, uv (package manager), Apache Spark 3.5 + Delta Lake 3.x, Apache Kafka, Apache Airflow 2.10, AWS (S3, EMR, EC2), Terraform 1.9, Neo4j 5.x Community, MinIO (dev), GLiNER, REBEL, sentence-transformers, CLIP ViT-B/32, Claude Haiku 4.5 via Anthropic SDK, LangChain `LLMGraphTransformer`, Great Expectations.

**Reference spec:** `/Users/massimo/.claude/plans/dovremmo-ideare-un-plan-sleepy-tower.md` (da committare in `docs/superpowers/specs/2026-05-27-reddit-kg-pipeline-design.md` come prima azione di W1).

---

# Part A — Master Roadmap W1-W8

Roadmap a granularità "task unit" (mezza giornata - 2 giorni ciascuno). Ogni settimana produce un deliverable testabile in isolamento.

**Divisione del lavoro per team di 2 persone:**
- **Dev A** (Data Engineering focus): infra AWS, Spark, Kafka, ingestion, cleaning, Airflow, Neo4j loading.
- **Dev B** (ML/NLP focus): schema discovery LLM, modelli HuggingFace (GLiNER/REBEL/sentiment/stance), canonicalization, Wikidata, validators, image embeddings.
- W1 e W8 sono in **pair**; le altre settimane hanno parallelization indicata sotto.

## Sprint W1 — Bootstrap & Local Dev Environment

**Deliverable:** repo configurato, docker-compose locale funzionante (Kafka + MinIO + Neo4j + Spark + Airflow), Terraform AWS minimo, CI con linting/test.

**Pairing**: entrambi sui fondamentali. Dev A guida infra, Dev B configura ML stack.

- ✅ Detailed plan in **Part B Sprint W1-W2 — Task 1 to 14**.

## Sprint W2 — Bronze Ingestion End-to-End

**Deliverable:** PRAW producer scrive su Kafka, Spark Structured Streaming consuma e scrive su bronze (Delta Lake su MinIO/S3). Pushshift loader processa dump storico jsonl.gz su bronze. Airflow DAG entrambi.

**Parallelization:**
- Dev A: PRAW producer, Kafka→Bronze Spark streaming, Pushshift Spark loader, Airflow DAGs.
- Dev B: scaffolding modelli HuggingFace (download cache locali GLiNER/sentence-transformers/CLIP per test offline), dataset fixtures, primo schema PySpark condiviso.

- ✅ Detailed plan in **Part B Sprint W1-W2 — Task 15 to 26**.

## Sprint W3 — Silver Layer + Data Governance

**Deliverable:** bronze→silver Spark job che fa schema enforcement, dedup, deidentificazione SHA-256, normalizzazione testuale, language detection, quality checks con Great Expectations. Tabella silver Delta. Airflow DAG dedicato.

**Parallelization:**
- Dev A: Spark cleaning job (dedup + deidentification + normalizzazione + filtri qualità) + Delta schema constraints.
- Dev B: Great Expectations suite (quality_gx.py) + language detection wrapper (fasttext-langid) + topic clustering preliminare (BERTopic opzionale) per stratified sampling.

**Critical files:**
- `pipeline/cleaning/deduplication.py`
- `pipeline/cleaning/deidentification.py`
- `pipeline/cleaning/normalization.py`
- `pipeline/cleaning/quality_gx.py`
- `airflow/dags/bronze_to_silver_dag.py`
- `tests/unit/cleaning/` (~4 file di test)
- `tests/integration/test_silver_layer.py`

**Acceptance criteria:**
- Su un campione di 1000 post: silver scarta ≥ 10% per quality, ≥ 95% degli ID sono univoci dopo dedup, nessun username in chiaro nel silver.
- Great Expectations check passano (notebook con DataDocs).
- Test integrazione end-to-end bronze→silver passa.

## Sprint W4 — Phase 1: Schema Discovery (LLM agnostic)

**Deliverable:** sampling stratificato dal silver, prompt LLM agnostic, output `domain_schema_v1.json` validato, salvato in S3 + Git, integrato in Airflow DAG.

**Parallelization:**
- Dev A: sampler Spark (stratified 60/30/10) + Airflow DAG + storage versionato + LLM client wrapper con retry/cache.
- Dev B: prompt engineering Claude Haiku, JSONSchema dello schema di output, validator JSON, human-in-the-loop review UI minimale (Streamlit one-shot o Airflow XCom + Slack notification).

**Critical files:**
- `pipeline/phase1/sampler.py`
- `pipeline/phase1/schema_discovery.py` (chiama Anthropic SDK)
- `pipeline/phase1/schema_validator.py` (JSONSchema check)
- `pipeline/phase1/prompts/agnostic_discovery.txt`
- `pipeline/common/llm_client.py` (wrapper Anthropic con caching + retry)
- `airflow/dags/phase1_schema_discovery_dag.py`
- `tests/unit/phase1/` (~3 file)
- `tests/integration/test_phase1_e2e.py`

**Acceptance criteria:**
- Su sample r/PokemonPlatinum: schema scoperto contiene tipi attesi (Pokemon, Move, Item, Location, Strategy, Tip) — confermare manualmente.
- Replicato su r/chess: schema diverso ma forma JSON identica (test agnosticity).
- Costo LLM: ≤ 2$.

## Sprint W5-W6 — Phase 2: Teacher-Student Extraction + Canonicalization

**Deliverable:** estrazione di entità e claim su tutto il silver, con teacher LLM su sample (~5-10K post) + student GLiNER/REBEL/heuristic sul resto. Sentiment + stance per claim. Canonicalization DBSCAN + Wikidata linking. Tabelle gold popolate.

**Parallelization (alta):**
- Dev A:
  - Spark UDF per GLiNER + REBEL distribuiti (`mapPartitions`).
  - Heuristica spaCy co-occurrence + dependency parsing.
  - Spark job per merge teacher+student + confidence combinata.
  - Canonicalization DBSCAN distribuito.
- Dev B:
  - LangChain `LLMGraphTransformer` setup teacher con schema da Phase 1.
  - Sentiment classifier wrapper (twitter-roberta).
  - Stance NLI wrapper (DeBERTa-v3-MNLI).
  - Wikidata SPARQL client con caching aggressivo (SQLite local cache).
  - Claim aggregator (CONSENSUS/CONTESTED/NICHE) Spark job.

**Critical files:**
- `pipeline/phase2/teacher_llm_extract.py`
- `pipeline/phase2/student_gliner.py`
- `pipeline/phase2/student_rebel.py`
- `pipeline/phase2/student_heuristics.py`
- `pipeline/phase2/extraction_merger.py`
- `pipeline/phase2/sentiment_stance.py`
- `pipeline/phase2/canonicalization.py`
- `pipeline/phase2/wikidata_linker.py`
- `pipeline/phase2/claim_aggregator.py`
- `airflow/dags/phase2_extraction_dag.py`
- `tests/unit/phase2/` (~6 file)
- `tests/integration/test_phase2_e2e.py`

**Acceptance criteria:**
- Inter-extractor agreement teacher↔student F1 ≥ 0.7 su sample comune.
- Schema adherence ≥ 95% nelle triple gold.
- Almeno il 30% delle entità canonicalizzate ha un Q-ID Wikidata su r/PokemonPlatinum.
- Sentiment e stance hanno distribuzione plausibile (non tutto neutrale).
- Costo LLM teacher ≤ 10$.

## Sprint W7 — Image Pipeline

**Deliverable:** download + dedup phash + CLIP embedding di tutte le immagini, linking a entità testuali via CLIP text-image similarity, scrittura su tabella gold `images_indexed`.

**Parallelization:**
- Dev A: downloader Spark async + backoff + retry, dedup phash, Airflow DAG, gestione GPU spot.
- Dev B: CLIP embedder (image + text encoder), entity-image linker con threshold tuning, valutazione manuale linking quality.

**Critical files:**
- `pipeline/images/downloader.py`
- `pipeline/images/phash_dedup.py`
- `pipeline/images/clip_embedder.py`
- `pipeline/images/entity_image_linker.py`
- `airflow/dags/images_pipeline_dag.py`
- `tests/unit/images/` (~4 file)
- `tests/integration/test_images_e2e.py`

**Acceptance criteria:**
- 100 link entity→image revisionati manualmente: precision ≥ 0.7.
- Dedup phash riduce le immagini di ≥ 30% (Reddit ha molti repost).
- Costo GPU spot ≤ 5$.

## Sprint W8 — Neo4j Loading + Validation + Report (Pairing)

**Deliverable:** caricamento KG in Neo4j Community con vector index nativi, query Cypher demo, notebook validators (PokeAPI + cross-domain r/chess), interface `KGRetriever` stub documentata, report finale per il corso.

**Parallelization (split poi merge):**
- Dev A:
  - `neo4j-spark-connector` loader con UNWIND batch.
  - Vector index creation su Post.embedding, Entity.embedding, Image.embedding.
  - Cypher query examples (top-10 strategie Cynthia, Pokemon più contestati, workaround più upvotati).
  - End-to-end pipeline run su r/PokemonPlatinum.
- Dev B:
  - Notebook `validators/pokeapi_demo.ipynb` con metriche precision/recall.
  - Pipeline run su r/chess per dimostrazione agnosticity.
  - Notebook `from_zero_to_kg.ipynb` per replicabilità.
  - Stub `rag/kg_retriever.py` con docstring + interface signatures.
  - Report tecnico finale (markdown → PDF) per il corso.

**Critical files:**
- `pipeline/neo4j_loader/load_kg.py`
- `pipeline/neo4j_loader/vector_indexes.py`
- `pipeline/neo4j_loader/cypher_queries/` (cartella con `.cypher` files)
- `rag/kg_retriever.py`
- `validators/pokeapi_demo.ipynb`
- `notebooks/from_zero_to_kg.ipynb`
- `notebooks/eval_kg_quality.ipynb`
- `docs/report/final_report.md`

**Acceptance criteria:**
- KG navigabile in Neo4j Browser con visualizzazione delle relazioni discourse.
- Vector index funzionanti (query di prova restituisce risultati).
- PokeAPI validator precision ≥ 0.6 sulle entità Pokemon estratte.
- Pipeline gira end-to-end su r/chess senza modifiche al codice.
- Report finale review-ready.

## Master timeline summary

| Sprint | Settimane | Deliverable | Costo AWS stimato |
|--------|-----------|-------------|-------------------|
| W1 | 1 | Bootstrap + dev env | ~0$ (locale) |
| W2 | 2 | Bronze ingestion | ~10-15$ |
| W3 | 3 | Silver + governance | ~10-15$ |
| W4 | 4 | Phase 1 schema discovery | ~5$ (LLM) |
| W5-6 | 5-6 | Phase 2 extraction + canonicalization | ~15-25$ |
| W7 | 7 | Image pipeline | ~5-8$ |
| W8 | 8 | Neo4j + validation + report | ~5-10$ |
| **Totale** | **8 settimane** | **Pipeline completa subreddit→KG** | **~50-78$ + buffer 20$** |

---

# Part B — Sprint W1-W2 Detailed Plan (eseguibile subito)

Pianificazione bite-sized per i primi 2 sprint. Ogni task ha checkbox, file esatti, codice completo, comandi e expected output. Niente placeholder.

## Convenzioni

- **Python**: 3.11.
- **Package manager**: `uv` (rapido, lockfile riproducibile).
- **Style**: ruff per linting + formatting.
- **Test**: pytest + pytest-cov + hypothesis (property-based) + testcontainers (per Kafka/Spark integration).
- **Commits**: convenzione Conventional Commits (`feat:`, `fix:`, `chore:`, `test:`, `docs:`, `refactor:`).
- **Git**: una branch per sprint (`sprint/w1-bootstrap`, `sprint/w2-bronze`), PR e merge a fine sprint.
- **Working directory**: `/Users/massimo/Documents/dev/big-data/primo-progetto/`.

## File structure target alla fine di W2

```
primo-progetto/
├── .env.example
├── .gitignore
├── .python-version
├── pyproject.toml
├── uv.lock
├── ruff.toml
├── pytest.ini
├── README.md
├── docs/
│   └── superpowers/
│       ├── specs/
│       │   └── 2026-05-27-reddit-kg-pipeline-design.md
│       └── plans/
│           └── 2026-05-27-reddit-kg-pipeline-implementation.md
├── docker/
│   ├── docker-compose.yml
│   ├── airflow/
│   │   └── Dockerfile
│   └── README.md
├── infra/
│   └── terraform/
│       ├── main.tf
│       ├── variables.tf
│       ├── outputs.tf
│       └── README.md
├── pipeline/
│   ├── __init__.py
│   ├── common/
│   │   ├── __init__.py
│   │   ├── config.py
│   │   ├── logging.py
│   │   └── storage.py
│   └── ingestion/
│       ├── __init__.py
│       ├── praw_producer.py
│       ├── kafka_to_bronze.py
│       ├── pushshift_loader.py
│       └── schemas.py
├── airflow/
│   ├── dags/
│   │   ├── __init__.py
│   │   ├── ingest_pushshift_dag.py
│   │   └── ingest_praw_streaming_dag.py
│   └── requirements.txt
└── tests/
    ├── __init__.py
    ├── conftest.py
    ├── fixtures/
    │   ├── __init__.py
    │   ├── sample_pushshift.jsonl.gz
    │   └── sample_praw_post.json
    ├── unit/
    │   ├── __init__.py
    │   └── ingestion/
    │       ├── __init__.py
    │       ├── test_praw_producer.py
    │       ├── test_kafka_to_bronze.py
    │       ├── test_pushshift_loader.py
    │       └── test_schemas.py
    └── integration/
        ├── __init__.py
        └── test_e2e_bronze.py
```

---

### Task 1: Repo Initialization + Git

**Files:**
- Create: `.gitignore`
- Create: `.python-version`
- Create: `README.md`
- Create: `docs/superpowers/specs/2026-05-27-reddit-kg-pipeline-design.md` (copia dalla spec approvata)

- [ ] **Step 1: Inizializza git repository**

```bash
cd /Users/massimo/Documents/dev/big-data/primo-progetto
git init
git checkout -b main
```

- [ ] **Step 2: Crea `.gitignore`**

```gitignore
# Python
__pycache__/
*.py[cod]
*$py.class
*.so
.Python
.venv/
venv/
env/
.uv-cache/
*.egg-info/
.pytest_cache/
.coverage
htmlcov/
.ruff_cache/

# Spark / Delta
spark-warehouse/
metastore_db/
derby.log
_delta_log/

# Secrets
.env
.env.local
*.pem
*.key
credentials.json

# IDE
.idea/
.vscode/
*.swp
.DS_Store

# Data
data/
*.parquet
*.delta
*.jsonl
*.jsonl.gz
!tests/fixtures/*.jsonl
!tests/fixtures/*.jsonl.gz

# Terraform
infra/terraform/.terraform/
infra/terraform/*.tfstate
infra/terraform/*.tfstate.backup
infra/terraform/*.tfvars
!infra/terraform/*.tfvars.example

# Airflow
airflow/logs/
airflow/airflow.db
airflow/airflow.cfg

# Misc
.cache/
*.log
```

- [ ] **Step 3: Crea `.python-version`**

```
3.11
```

- [ ] **Step 4: Crea `README.md`**

```markdown
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
```

- [ ] **Step 5: Copia la spec approvata in `docs/superpowers/specs/`**

```bash
mkdir -p docs/superpowers/specs
cp /Users/massimo/.claude/plans/dovremmo-ideare-un-plan-sleepy-tower.md \
   docs/superpowers/specs/2026-05-27-reddit-kg-pipeline-design.md
```

- [ ] **Step 6: Primo commit**

```bash
git add .gitignore .python-version README.md docs/
git commit -m "chore: initialize repository with spec and project docs"
```

Expected: commit creato, `git log` mostra 1 commit.

---

### Task 2: Python Project with uv

**Files:**
- Create: `pyproject.toml`
- Create: `ruff.toml`
- Create: `pytest.ini`

- [ ] **Step 1: Installa uv se non presente**

```bash
which uv || curl -LsSf https://astral.sh/uv/install.sh | sh
```

Expected: `uv --version` ritorna ≥ 0.4.

- [ ] **Step 2: Crea `pyproject.toml`**

```toml
[project]
name = "reddit-kg-pipeline"
version = "0.1.0"
description = "Subreddit-agnostic pipeline producing a Community Discourse Knowledge Graph in Neo4j"
requires-python = ">=3.11,<3.12"
authors = [{ name = "Massimo + team" }]

dependencies = [
    # Distributed
    "pyspark==3.5.1",
    "delta-spark==3.2.0",
    "kafka-python==2.0.2",
    "confluent-kafka==2.5.0",

    # Reddit
    "praw==7.7.1",

    # AWS
    "boto3==1.34.0",
    "s3fs==2024.6.1",

    # Common
    "pydantic==2.8.2",
    "pydantic-settings==2.4.0",
    "structlog==24.4.0",
    "click==8.1.7",
    "python-dotenv==1.0.1",
]

[project.optional-dependencies]
dev = [
    "pytest==8.3.2",
    "pytest-cov==5.0.0",
    "pytest-mock==3.14.0",
    "hypothesis==6.111.0",
    "testcontainers[kafka]==4.7.2",
    "ruff==0.6.2",
    "ipython==8.26.0",
    "jupyter==1.0.0",
]

ml = [
    # Solo per dev locale dei modelli, non in run distribuito
    "transformers==4.44.0",
    "sentence-transformers==3.0.1",
    "gliner==0.2.10",
    "torch==2.4.0",
    "anthropic==0.34.0",
    "langchain-experimental==0.0.65",
    "langchain-anthropic==0.1.23",
]

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[tool.hatch.build.targets.wheel]
packages = ["pipeline"]
```

- [ ] **Step 3: Crea `ruff.toml`**

```toml
line-length = 100
target-version = "py311"

[lint]
select = [
    "E", "F", "W",   # pycodestyle, pyflakes
    "I",             # isort
    "UP",            # pyupgrade
    "B",             # bugbear
    "SIM",           # simplify
    "PT",            # pytest
    "RET",           # return
    "PTH",           # pathlib
]
ignore = ["E501"]  # line too long handled by formatter

[format]
quote-style = "double"
```

- [ ] **Step 4: Crea `pytest.ini`**

```ini
[pytest]
testpaths = tests
python_files = test_*.py
python_classes = Test*
python_functions = test_*
addopts =
    -ra
    --strict-markers
    --strict-config
    --cov=pipeline
    --cov-report=term-missing
    --cov-report=html
markers =
    unit: unit tests (default)
    integration: integration tests (requires docker-compose)
    slow: slow tests (>5s)
```

- [ ] **Step 5: Sync dependencies**

```bash
uv sync --extra dev
```

Expected: `.venv/` creato, `uv.lock` generato, nessun errore.

- [ ] **Step 6: Verifica installazione**

```bash
uv run python -c "import pyspark, praw, kafka; print('OK')"
uv run ruff --version
uv run pytest --version
```

Expected: stampa `OK`, ruff ≥0.6, pytest ≥8.

- [ ] **Step 7: Commit**

```bash
git add pyproject.toml uv.lock ruff.toml pytest.ini
git commit -m "chore: setup Python project with uv, ruff, pytest"
```

---

### Task 3: Common Modules (config, logging, storage)

**Files:**
- Create: `pipeline/__init__.py`
- Create: `pipeline/common/__init__.py`
- Create: `pipeline/common/config.py`
- Create: `pipeline/common/logging.py`
- Create: `pipeline/common/storage.py`
- Create: `tests/__init__.py`
- Create: `tests/conftest.py`
- Create: `tests/unit/__init__.py`
- Create: `tests/unit/common/__init__.py`
- Create: `tests/unit/common/test_config.py`
- Create: `.env.example`

- [ ] **Step 1: Test per `config.py` (TDD)**

`tests/unit/common/test_config.py`:

```python
import pytest

from pipeline.common.config import Settings


def test_settings_loads_from_env(monkeypatch):
    monkeypatch.setenv("REDDIT_CLIENT_ID", "abc")
    monkeypatch.setenv("REDDIT_CLIENT_SECRET", "xyz")
    monkeypatch.setenv("REDDIT_USER_AGENT", "test-bot/0.1 by u/test")
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    monkeypatch.setenv("S3_ENDPOINT_URL", "http://minio:9000")
    monkeypatch.setenv("S3_BRONZE_BUCKET", "bronze")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "minioadmin")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "minioadmin")
    monkeypatch.setenv("PIPELINE_VERSION", "0.1.0")

    s = Settings()

    assert s.reddit_client_id == "abc"
    assert s.kafka_bootstrap_servers == "localhost:9092"
    assert s.s3_bronze_bucket == "bronze"
    assert s.pipeline_version == "0.1.0"


def test_settings_missing_required_raises(monkeypatch):
    monkeypatch.delenv("REDDIT_CLIENT_ID", raising=False)
    with pytest.raises(Exception):
        Settings()
```

- [ ] **Step 2: Run test → atteso FAIL (module not found)**

```bash
uv run pytest tests/unit/common/test_config.py -v
```

Expected: ImportError o ModuleNotFoundError.

- [ ] **Step 3: Implementa `pipeline/common/config.py`**

```python
"""Centralized configuration via environment variables."""

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment.

    Required env vars must be set (no defaults for secrets).
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Reddit
    reddit_client_id: str = Field(..., alias="REDDIT_CLIENT_ID")
    reddit_client_secret: str = Field(..., alias="REDDIT_CLIENT_SECRET")
    reddit_user_agent: str = Field(..., alias="REDDIT_USER_AGENT")

    # Kafka
    kafka_bootstrap_servers: str = Field(..., alias="KAFKA_BOOTSTRAP_SERVERS")
    kafka_topic_reddit_raw: str = Field("reddit-raw", alias="KAFKA_TOPIC_REDDIT_RAW")

    # S3 / MinIO
    s3_endpoint_url: str | None = Field(None, alias="S3_ENDPOINT_URL")
    s3_bronze_bucket: str = Field(..., alias="S3_BRONZE_BUCKET")
    s3_silver_bucket: str = Field("silver", alias="S3_SILVER_BUCKET")
    s3_gold_bucket: str = Field("gold", alias="S3_GOLD_BUCKET")
    aws_access_key_id: str = Field(..., alias="AWS_ACCESS_KEY_ID")
    aws_secret_access_key: str = Field(..., alias="AWS_SECRET_ACCESS_KEY")
    aws_region: str = Field("us-east-1", alias="AWS_REGION")

    # Pipeline metadata
    pipeline_version: str = Field(..., alias="PIPELINE_VERSION")
    pii_salt: str = Field("dev-salt-change-me", alias="PII_SALT")
```

- [ ] **Step 4: Crea `pipeline/__init__.py`**

```python
"""Reddit → KG pipeline."""

__version__ = "0.1.0"
```

`pipeline/common/__init__.py`:

```python
from pipeline.common.config import Settings

__all__ = ["Settings"]
```

- [ ] **Step 5: Crea `tests/__init__.py`, `tests/unit/__init__.py`, `tests/unit/common/__init__.py`, `tests/conftest.py`**

`tests/conftest.py`:

```python
"""Shared pytest fixtures."""

import pytest


@pytest.fixture(autouse=True)
def _isolate_env(monkeypatch):
    """Ensure tests don't accidentally inherit user env vars."""
    for var in [
        "REDDIT_CLIENT_ID",
        "REDDIT_CLIENT_SECRET",
        "REDDIT_USER_AGENT",
        "KAFKA_BOOTSTRAP_SERVERS",
        "S3_ENDPOINT_URL",
        "S3_BRONZE_BUCKET",
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "PIPELINE_VERSION",
    ]:
        monkeypatch.delenv(var, raising=False)
```

I tre `__init__.py` sono file vuoti.

- [ ] **Step 6: Run test → PASS**

```bash
uv run pytest tests/unit/common/test_config.py -v
```

Expected: 2 passed.

- [ ] **Step 7: Implementa `logging.py` (no test, è wiring banale)**

`pipeline/common/logging.py`:

```python
"""Structured logging setup."""

import logging
import sys

import structlog


def configure_logging(level: str = "INFO") -> None:
    """Configure structlog + stdlib logging."""
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=getattr(logging, level.upper()),
    )
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(
            getattr(logging, level.upper())
        ),
        cache_logger_on_first_use=True,
    )


def get_logger(name: str) -> structlog.stdlib.BoundLogger:
    return structlog.get_logger(name)
```

(`structlog==24.4.0` è già in `dependencies` da Task 2.)

- [ ] **Step 8: Implementa `storage.py` (abstract S3/MinIO helpers)**

`pipeline/common/storage.py`:

```python
"""S3/MinIO storage helpers.

Wraps boto3 to provide a unified interface that works against both
real S3 and a local MinIO instance (transparent to callers).
"""

from __future__ import annotations

import boto3
from botocore.client import Config

from pipeline.common.config import Settings


def get_s3_client(settings: Settings):
    """Return an S3 client configured for MinIO or AWS S3."""
    kwargs = {
        "service_name": "s3",
        "aws_access_key_id": settings.aws_access_key_id,
        "aws_secret_access_key": settings.aws_secret_access_key,
        "region_name": settings.aws_region,
        "config": Config(signature_version="s3v4"),
    }
    if settings.s3_endpoint_url:
        kwargs["endpoint_url"] = settings.s3_endpoint_url
    return boto3.client(**kwargs)


def s3a_path(bucket: str, prefix: str = "") -> str:
    """Return an `s3a://` path for use with Spark."""
    prefix = prefix.lstrip("/")
    return f"s3a://{bucket}/{prefix}" if prefix else f"s3a://{bucket}"
```

- [ ] **Step 9: Crea `.env.example`**

```ini
# Reddit API (https://www.reddit.com/prefs/apps)
REDDIT_CLIENT_ID=your_client_id_here
REDDIT_CLIENT_SECRET=your_client_secret_here
REDDIT_USER_AGENT=reddit-kg-bot/0.1 by u/yourusername

# Kafka
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC_REDDIT_RAW=reddit-raw

# S3 / MinIO
S3_ENDPOINT_URL=http://localhost:9000
S3_BRONZE_BUCKET=bronze
S3_SILVER_BUCKET=silver
S3_GOLD_BUCKET=gold
AWS_ACCESS_KEY_ID=minioadmin
AWS_SECRET_ACCESS_KEY=minioadmin
AWS_REGION=us-east-1

# Pipeline
PIPELINE_VERSION=0.1.0
PII_SALT=change-me-in-prod
```

- [ ] **Step 10: Commit**

```bash
git add pipeline/ tests/ .env.example pyproject.toml
git commit -m "feat(common): add config, logging, storage modules"
```

---

### Task 4: Docker Compose Dev Stack

**Files:**
- Create: `docker/docker-compose.yml`
- Create: `docker/README.md`
- Create: `infra/scripts/init_minio.sh`

- [ ] **Step 1: Crea `docker/docker-compose.yml`**

```yaml
name: reddit-kg-dev

services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.6.1
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
    healthcheck:
      test: ["CMD", "nc", "-z", "localhost", "2181"]
      interval: 5s
      timeout: 5s
      retries: 10

  kafka:
    image: confluentinc/cp-kafka:7.6.1
    depends_on:
      zookeeper:
        condition: service_healthy
    ports:
      - "9092:9092"
      - "29092:29092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"
    healthcheck:
      test: ["CMD", "kafka-topics", "--bootstrap-server", "localhost:9092", "--list"]
      interval: 10s
      timeout: 10s
      retries: 10

  minio:
    image: minio/minio:RELEASE.2024-08-17T01-24-54Z
    ports:
      - "9000:9000"
      - "9001:9001"
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin
    volumes:
      - minio_data:/data
    command: server /data --console-address ":9001"
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:9000/minio/health/live"]
      interval: 10s
      timeout: 5s
      retries: 10

  minio-init:
    image: minio/mc:RELEASE.2024-08-17T11-33-50Z
    depends_on:
      minio:
        condition: service_healthy
    entrypoint: >
      /bin/sh -c "
      mc alias set local http://minio:9000 minioadmin minioadmin;
      mc mb --ignore-existing local/bronze;
      mc mb --ignore-existing local/silver;
      mc mb --ignore-existing local/gold;
      mc mb --ignore-existing local/raw-images;
      echo 'MinIO buckets created.';
      "

  neo4j:
    image: neo4j:5.23-community
    ports:
      - "7474:7474"
      - "7687:7687"
    environment:
      NEO4J_AUTH: neo4j/devpassword
      NEO4J_PLUGINS: '["apoc"]'
      NEO4J_dbms_security_procedures_unrestricted: "apoc.*"
    volumes:
      - neo4j_data:/data
      - neo4j_logs:/logs
    healthcheck:
      test: ["CMD", "wget", "-q", "--spider", "http://localhost:7474"]
      interval: 10s
      timeout: 5s
      retries: 10

  spark:
    image: bitnami/spark:3.5.1
    environment:
      SPARK_MODE: master
    ports:
      - "8080:8080"
      - "7077:7077"

  spark-worker:
    image: bitnami/spark:3.5.1
    depends_on:
      - spark
    environment:
      SPARK_MODE: worker
      SPARK_MASTER_URL: spark://spark:7077
      SPARK_WORKER_MEMORY: 2G
      SPARK_WORKER_CORES: 2

volumes:
  minio_data:
  neo4j_data:
  neo4j_logs:
```

- [ ] **Step 2: Crea `docker/README.md`**

```markdown
# Local Dev Stack

## Start

```bash
cd docker
docker compose up -d
```

## Endpoints

- Kafka broker: `localhost:9092`
- MinIO API: `http://localhost:9000` (admin/password: `minioadmin`/`minioadmin`)
- MinIO console: `http://localhost:9001`
- Neo4j Browser: `http://localhost:7474` (neo4j/devpassword)
- Neo4j Bolt: `bolt://localhost:7687`
- Spark master UI: `http://localhost:8080`

## Buckets created on startup

`bronze`, `silver`, `gold`, `raw-images` (via `minio-init` service).

## Stop

```bash
docker compose down       # keep volumes
docker compose down -v    # also drop volumes (full reset)
```
```

- [ ] **Step 3: Avvia lo stack e verifica health**

```bash
cd /Users/massimo/Documents/dev/big-data/primo-progetto/docker
docker compose up -d
sleep 30
docker compose ps
```

Expected: 7 servizi `running` o `healthy`.

- [ ] **Step 4: Verifica connettività Kafka e MinIO**

```bash
# Kafka: lista topic
docker compose exec kafka kafka-topics --bootstrap-server localhost:9092 --list

# MinIO: lista bucket
docker compose run --rm minio-init mc ls local/
```

Expected: comandi tornano senza errore; MinIO mostra 4 bucket.

- [ ] **Step 5: Commit**

```bash
cd ..
git add docker/
git commit -m "chore(docker): add local dev stack (Kafka, MinIO, Neo4j, Spark)"
```

---

### Task 5: PySpark Bronze Schemas

**Files:**
- Create: `pipeline/ingestion/__init__.py`
- Create: `pipeline/ingestion/schemas.py`
- Create: `tests/unit/ingestion/__init__.py`
- Create: `tests/unit/ingestion/test_schemas.py`

- [ ] **Step 1: Test schemas (TDD)**

`tests/unit/ingestion/test_schemas.py`:

```python
from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    LongType,
    StringType,
    StructType,
)

from pipeline.ingestion.schemas import bronze_post_schema, ingestion_metadata_fields


def test_bronze_schema_has_core_reddit_fields():
    schema = bronze_post_schema()
    names = {f.name for f in schema.fields}
    expected = {
        "id",
        "subreddit",
        "author",
        "title",
        "selftext",
        "body",
        "created_utc",
        "score",
        "is_self",
        "parent_id",
        "permalink",
        "url",
        "kind",  # 'post' or 'comment'
    }
    assert expected.issubset(names)


def test_bronze_schema_has_ingestion_metadata():
    schema = bronze_post_schema()
    names = {f.name for f in schema.fields}
    assert "ingest_ts" in names
    assert "ingest_source" in names
    assert "pipeline_version" in names


def test_created_utc_is_long():
    schema = bronze_post_schema()
    f = next(f for f in schema.fields if f.name == "created_utc")
    assert isinstance(f.dataType, LongType)


def test_score_is_integer():
    schema = bronze_post_schema()
    f = next(f for f in schema.fields if f.name == "score")
    assert isinstance(f.dataType, IntegerType)


def test_is_self_is_boolean():
    schema = bronze_post_schema()
    f = next(f for f in schema.fields if f.name == "is_self")
    assert isinstance(f.dataType, BooleanType)


def test_ingestion_metadata_fields_returns_three():
    fields = ingestion_metadata_fields()
    assert len(fields) == 3
    assert all(isinstance(f.dataType, StringType) for f in fields if f.name != "ingest_ts")
```

- [ ] **Step 2: Run test → atteso FAIL**

```bash
uv run pytest tests/unit/ingestion/test_schemas.py -v
```

Expected: ModuleNotFoundError.

- [ ] **Step 3: Implementa `pipeline/ingestion/schemas.py`**

```python
"""PySpark schemas for the bronze layer.

The bronze schema accepts both Reddit posts and comments in a single
unified shape — the `kind` column distinguishes them. Some fields are
null for one or the other (e.g. `title` is null for comments).
"""

from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


def ingestion_metadata_fields() -> list[StructField]:
    """Metadata fields appended to every bronze record at ingestion time."""
    return [
        StructField("ingest_ts", TimestampType(), nullable=False),
        StructField("ingest_source", StringType(), nullable=False),  # 'praw' | 'pushshift'
        StructField("pipeline_version", StringType(), nullable=False),
    ]


def bronze_post_schema() -> StructType:
    """Unified bronze schema for Reddit posts AND comments."""
    return StructType(
        [
            StructField("id", StringType(), nullable=False),
            StructField("subreddit", StringType(), nullable=False),
            StructField("author", StringType(), nullable=True),
            StructField("title", StringType(), nullable=True),
            StructField("selftext", StringType(), nullable=True),
            StructField("body", StringType(), nullable=True),
            StructField("created_utc", LongType(), nullable=False),
            StructField("score", IntegerType(), nullable=True),
            StructField("is_self", BooleanType(), nullable=True),
            StructField("parent_id", StringType(), nullable=True),
            StructField("permalink", StringType(), nullable=True),
            StructField("url", StringType(), nullable=True),
            StructField("kind", StringType(), nullable=False),  # 'post' | 'comment'
            *ingestion_metadata_fields(),
        ]
    )
```

- [ ] **Step 4: Crea `pipeline/ingestion/__init__.py`**

```python
"""Ingestion: PRAW streaming + Pushshift batch → bronze layer."""

from pipeline.ingestion.schemas import bronze_post_schema, ingestion_metadata_fields

__all__ = ["bronze_post_schema", "ingestion_metadata_fields"]
```

- [ ] **Step 5: Crea `tests/unit/ingestion/__init__.py`** (vuoto).

- [ ] **Step 6: Run test → PASS**

```bash
uv run pytest tests/unit/ingestion/test_schemas.py -v
```

Expected: 6 passed.

- [ ] **Step 7: Commit**

```bash
git add pipeline/ingestion/ tests/unit/ingestion/
git commit -m "feat(ingestion): add unified bronze PySpark schema"
```

---

### Task 6: PRAW Producer — Skeleton

**Files:**
- Create: `pipeline/ingestion/praw_producer.py`
- Create: `tests/unit/ingestion/test_praw_producer.py`
- Create: `tests/fixtures/__init__.py`
- Create: `tests/fixtures/sample_praw_post.json`

- [ ] **Step 1: Fixture sample PRAW post**

`tests/fixtures/sample_praw_post.json`:

```json
{
  "id": "abc123",
  "subreddit": "PokemonPlatinum",
  "author": "trainer_red",
  "title": "Best moveset for Garchomp?",
  "selftext": "I just caught a Gible and want to evolve it. What's the optimal moveset for Platinum endgame?",
  "created_utc": 1716800000,
  "score": 42,
  "is_self": true,
  "permalink": "/r/PokemonPlatinum/comments/abc123/best_moveset_for_garchomp/",
  "url": "https://www.reddit.com/r/PokemonPlatinum/comments/abc123/best_moveset_for_garchomp/"
}
```

- [ ] **Step 2: Test del produttore (mocked PRAW + mocked Kafka)**

`tests/unit/ingestion/test_praw_producer.py`:

```python
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
```

- [ ] **Step 3: Run test → atteso FAIL**

```bash
uv run pytest tests/unit/ingestion/test_praw_producer.py -v
```

Expected: ImportError.

- [ ] **Step 4: Implementa `pipeline/ingestion/praw_producer.py`**

```python
"""PRAW producer: poll Reddit API and publish to Kafka.

Designed to be invoked periodically by Airflow (e.g. every 5 minutes).
Idempotency: Kafka topic acts as the immutable log; downstream Spark
Structured Streaming deduplicates on (id, subreddit).
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any, Iterable

import praw
from kafka import KafkaProducer

from pipeline.common.logging import get_logger

log = get_logger(__name__)


def serialize_submission(
    sub: Any,
    *,
    ingest_source: str,
    pipeline_version: str,
) -> dict[str, Any]:
    """Map a PRAW Submission/Comment object to the bronze schema shape.

    `kind` is normalized to 'post' or 'comment'. PRAW internal kinds
    (`t1`=comment, `t3`=post) are mapped here.
    """
    raw_kind = getattr(sub, "kind", "t3")
    kind = "comment" if raw_kind == "t1" else "post"

    return {
        "id": sub.id,
        "subreddit": str(sub.subreddit),
        "author": getattr(sub.author, "name", None) if sub.author else None,
        "title": getattr(sub, "title", None),
        "selftext": getattr(sub, "selftext", None),
        "body": getattr(sub, "body", None),
        "created_utc": int(sub.created_utc),
        "score": getattr(sub, "score", None),
        "is_self": getattr(sub, "is_self", None),
        "parent_id": getattr(sub, "parent_id", None),
        "permalink": getattr(sub, "permalink", None),
        "url": getattr(sub, "url", None),
        "kind": kind,
        "ingest_ts": datetime.now(UTC).isoformat(),
        "ingest_source": ingest_source,
        "pipeline_version": pipeline_version,
    }


class PrawProducer:
    """Fetch new submissions/comments from a subreddit and publish to Kafka."""

    def __init__(
        self,
        *,
        reddit: praw.Reddit,
        kafka_producer: KafkaProducer,
        topic: str,
        pipeline_version: str,
    ) -> None:
        self._reddit = reddit
        self._kafka = kafka_producer
        self._topic = topic
        self._pipeline_version = pipeline_version

    def fetch_and_publish(self, *, subreddit: str, limit: int = 100) -> int:
        """Fetch up to `limit` newest items from `subreddit` and publish."""
        sub = self._reddit.subreddit(subreddit)
        count = 0
        for item in sub.new(limit=limit):
            payload = serialize_submission(
                item,
                ingest_source="praw",
                pipeline_version=self._pipeline_version,
            )
            self._kafka.send(
                self._topic,
                key=payload["id"].encode("utf-8"),
                value=json.dumps(payload).encode("utf-8"),
            )
            count += 1
        self._kafka.flush()
        log.info("praw_publish_complete", subreddit=subreddit, count=count)
        return count
```

- [ ] **Step 5: Run test → PASS**

```bash
uv run pytest tests/unit/ingestion/test_praw_producer.py -v
```

Expected: 2 passed.

- [ ] **Step 6: Commit**

```bash
git add pipeline/ingestion/praw_producer.py tests/unit/ingestion/test_praw_producer.py tests/fixtures/
git commit -m "feat(ingestion): add PRAW producer with Kafka publishing"
```

---

### Task 7: PRAW Producer — CLI Entry Point

**Files:**
- Create: `pipeline/ingestion/cli.py`
- Modify: `pyproject.toml` (add CLI entry point)

- [ ] **Step 1: Implementa CLI entry point**

`pipeline/ingestion/cli.py`:

```python
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
```

- [ ] **Step 2: Aggiungi entry point in `pyproject.toml`**

Aggiungi sotto `[project]`:

```toml
[project.scripts]
ingest = "pipeline.ingestion.cli:cli"
```

Re-sync:

```bash
uv sync --extra dev
```

- [ ] **Step 3: Manual smoke test (richiede credenziali Reddit + Kafka up)**

```bash
# Copia .env.example in .env e riempi con credenziali reali Reddit
cp .env.example .env
# (edita .env con credenziali Reddit + verifica Kafka up)

uv run ingest praw-fetch --subreddit PokemonPlatinum --limit 5
```

Expected: stampa `Published 5 items to reddit-raw`.

Verifica su Kafka:

```bash
docker compose -f docker/docker-compose.yml exec kafka \
  kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic reddit-raw --from-beginning --max-messages 5
```

Expected: 5 messaggi JSON Reddit visualizzati.

- [ ] **Step 4: Commit**

```bash
git add pipeline/ingestion/cli.py pyproject.toml
git commit -m "feat(ingestion): add PRAW fetch CLI command"
```

---

### Task 8: Kafka → Bronze Spark Structured Streaming

**Files:**
- Create: `pipeline/ingestion/kafka_to_bronze.py`
- Create: `tests/unit/ingestion/test_kafka_to_bronze.py`

- [ ] **Step 1: Test della trasformazione (no Spark session, pura logica)**

`tests/unit/ingestion/test_kafka_to_bronze.py`:

```python
import json

import pytest
from pyspark.sql import SparkSession

from pipeline.ingestion.kafka_to_bronze import parse_kafka_value


@pytest.fixture(scope="module")
def spark():
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("test-kafka-to-bronze")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield spark
    spark.stop()


def test_parse_kafka_value_returns_typed_columns(spark):
    payload = {
        "id": "abc123",
        "subreddit": "PokemonPlatinum",
        "author": "trainer_red",
        "title": "Title",
        "selftext": "body",
        "body": None,
        "created_utc": 1716800000,
        "score": 42,
        "is_self": True,
        "parent_id": None,
        "permalink": "/r/PokemonPlatinum/x/",
        "url": "https://reddit.com/x",
        "kind": "post",
        "ingest_ts": "2026-05-27T10:00:00+00:00",
        "ingest_source": "praw",
        "pipeline_version": "0.1.0",
    }
    raw = [(json.dumps(payload).encode("utf-8"),)]
    df = spark.createDataFrame(raw, "value BINARY")

    parsed = parse_kafka_value(df)
    row = parsed.collect()[0]

    assert row.id == "abc123"
    assert row.created_utc == 1716800000
    assert row.score == 42
    assert row.is_self is True
    assert row.kind == "post"
    assert row.pipeline_version == "0.1.0"
```

- [ ] **Step 2: Run test → atteso FAIL**

```bash
uv run pytest tests/unit/ingestion/test_kafka_to_bronze.py -v -m "not slow"
```

Expected: ImportError o test fallisce.

- [ ] **Step 3: Implementa `pipeline/ingestion/kafka_to_bronze.py`**

```python
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
```

- [ ] **Step 4: Run test → PASS**

```bash
uv run pytest tests/unit/ingestion/test_kafka_to_bronze.py -v
```

Expected: 1 passed (potrebbe richiedere Java installato per Spark locale, vedi note).

**Note:** se test fallisce per Java mancante: `brew install openjdk@17` su macOS, poi `export JAVA_HOME=$(/usr/libexec/java_home -v 17)`.

- [ ] **Step 5: Commit**

```bash
git add pipeline/ingestion/kafka_to_bronze.py tests/unit/ingestion/test_kafka_to_bronze.py
git commit -m "feat(ingestion): add Kafka→Bronze Spark Structured Streaming job"
```

---

### Task 9: Pushshift Loader (Spark Batch)

**Files:**
- Create: `pipeline/ingestion/pushshift_loader.py`
- Create: `tests/unit/ingestion/test_pushshift_loader.py`
- Create: `tests/fixtures/sample_pushshift.jsonl.gz` (sintetico)

- [ ] **Step 1: Genera fixture Pushshift sintetica**

Crea `tests/fixtures/_generate_pushshift_fixture.py`:

```python
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
```

Esegui una volta:

```bash
uv run python tests/fixtures/_generate_pushshift_fixture.py
```

- [ ] **Step 2: Test loader**

`tests/unit/ingestion/test_pushshift_loader.py`:

```python
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
```

- [ ] **Step 3: Run test → atteso FAIL**

```bash
uv run pytest tests/unit/ingestion/test_pushshift_loader.py -v
```

Expected: ImportError.

- [ ] **Step 4: Implementa `pipeline/ingestion/pushshift_loader.py`**

```python
"""Pushshift dump loader (Spark batch job).

Reads gzipped JSONL dumps of Reddit posts/comments and writes them to
the bronze Delta layer with full schema alignment.
"""

from __future__ import annotations

import click
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    coalesce,
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

    # Determine `kind`: Pushshift comments have `body`, posts have `selftext`/`title`.
    with_kind = raw.withColumn(
        "kind",
        when(raw["body"].isNotNull() & raw["title"].isNull(), lit("comment")).otherwise(lit("post")),
    )

    # Align to bronze schema: add missing columns as null
    schema = bronze_post_schema()
    field_names = {f.name for f in schema.fields}
    existing = set(with_kind.columns)
    for name in field_names - existing:
        # Special-cased fields below
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
```

- [ ] **Step 5: Run test → PASS**

```bash
uv run pytest tests/unit/ingestion/test_pushshift_loader.py -v
```

Expected: 1 passed.

- [ ] **Step 6: Commit**

```bash
git add pipeline/ingestion/pushshift_loader.py tests/unit/ingestion/test_pushshift_loader.py \
        tests/fixtures/sample_pushshift.jsonl.gz tests/fixtures/_generate_pushshift_fixture.py
git commit -m "feat(ingestion): add Pushshift Spark batch loader"
```

---

### Task 10: Airflow Local Setup

**Files:**
- Modify: `docker/docker-compose.yml` (aggiungi servizio Airflow)
- Create: `docker/airflow/Dockerfile`
- Create: `airflow/requirements.txt`
- Create: `airflow/dags/__init__.py`

- [ ] **Step 1: Crea `docker/airflow/Dockerfile`**

```dockerfile
FROM apache/airflow:2.10.0-python3.11

COPY airflow/requirements.txt /tmp/airflow-requirements.txt
RUN pip install --no-cache-dir -r /tmp/airflow-requirements.txt
```

- [ ] **Step 2: Crea `airflow/requirements.txt`**

```
apache-airflow-providers-apache-spark==4.10.0
apache-airflow-providers-amazon==8.27.0
apache-airflow-providers-apache-kafka==1.5.0
praw==7.7.1
kafka-python==2.0.2
pydantic==2.8.2
pydantic-settings==2.4.0
structlog==24.4.0
```

- [ ] **Step 3: Aggiungi Airflow servizi a `docker/docker-compose.yml`**

Aggiungi sotto i servizi esistenti:

```yaml
  airflow-postgres:
    image: postgres:16-alpine
    environment:
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: airflow
      POSTGRES_DB: airflow
    volumes:
      - airflow_pg:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD", "pg_isready", "-U", "airflow"]
      interval: 5s
      retries: 10

  airflow-init:
    build:
      context: ..
      dockerfile: docker/airflow/Dockerfile
    depends_on:
      airflow-postgres:
        condition: service_healthy
    environment:
      AIRFLOW__CORE__EXECUTOR: LocalExecutor
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@airflow-postgres:5432/airflow
      AIRFLOW__CORE__LOAD_EXAMPLES: "false"
    entrypoint: >
      /bin/bash -c "
      airflow db migrate &&
      airflow users create
        --username admin --password admin
        --firstname admin --lastname admin
        --role Admin --email admin@example.com || true
      "

  airflow-webserver:
    build:
      context: ..
      dockerfile: docker/airflow/Dockerfile
    depends_on:
      airflow-init:
        condition: service_completed_successfully
    ports:
      - "8081:8080"
    environment:
      AIRFLOW__CORE__EXECUTOR: LocalExecutor
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@airflow-postgres:5432/airflow
      AIRFLOW__CORE__LOAD_EXAMPLES: "false"
      AIRFLOW__WEBSERVER__SECRET_KEY: dev-secret-change-me
    volumes:
      - ../airflow/dags:/opt/airflow/dags
      - ../pipeline:/opt/airflow/pipeline
    command: webserver

  airflow-scheduler:
    build:
      context: ..
      dockerfile: docker/airflow/Dockerfile
    depends_on:
      airflow-init:
        condition: service_completed_successfully
    environment:
      AIRFLOW__CORE__EXECUTOR: LocalExecutor
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@airflow-postgres:5432/airflow
      AIRFLOW__CORE__LOAD_EXAMPLES: "false"
    volumes:
      - ../airflow/dags:/opt/airflow/dags
      - ../pipeline:/opt/airflow/pipeline
    command: scheduler
```

E aggiungi al blocco `volumes:` finale:

```yaml
  airflow_pg:
```

- [ ] **Step 4: Crea `airflow/dags/__init__.py`** (vuoto).

- [ ] **Step 5: Rebuild stack e verifica Airflow UI**

```bash
cd docker
docker compose build airflow-init airflow-webserver airflow-scheduler
docker compose up -d
sleep 60
docker compose ps
```

Apri `http://localhost:8081` (login `admin`/`admin`).

Expected: Airflow UI funzionante, nessun DAG (a parte gli example, disabilitati).

- [ ] **Step 6: Commit**

```bash
cd ..
git add docker/ airflow/
git commit -m "chore(airflow): add Airflow local stack with LocalExecutor"
```

---

### Task 11: Airflow DAG — PRAW Streaming

**Files:**
- Create: `airflow/dags/ingest_praw_streaming_dag.py`

- [ ] **Step 1: Implementa DAG**

`airflow/dags/ingest_praw_streaming_dag.py`:

```python
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
```

- [ ] **Step 2: Verifica DAG load (no Spark needed, BashOperator is enough)**

```bash
docker compose -f docker/docker-compose.yml exec airflow-webserver \
  airflow dags list | grep ingest_praw_streaming
```

Expected: la riga viene trovata.

- [ ] **Step 3: Run manuale del DAG (richiede `.env` con credenziali Reddit montato)**

Aggiungi a `airflow-webserver` e `airflow-scheduler` in docker-compose:

```yaml
    env_file:
      - ../.env
```

E rebuild:

```bash
docker compose up -d
docker compose exec airflow-webserver airflow dags trigger ingest_praw_streaming
```

Expected: DAG run schedulato, dopo ~30s entrambi i task completed.

Verifica:

```bash
docker compose exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic reddit-raw \
  --from-beginning --max-messages 5
```

Expected: messaggi visibili.

- [ ] **Step 4: Commit**

```bash
git add airflow/dags/ingest_praw_streaming_dag.py docker/docker-compose.yml
git commit -m "feat(airflow): add PRAW streaming DAG with 10-min schedule"
```

---

### Task 12: Airflow DAG — Pushshift Batch

**Files:**
- Create: `airflow/dags/ingest_pushshift_dag.py`

- [ ] **Step 1: Implementa DAG**

`airflow/dags/ingest_pushshift_dag.py`:

```python
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
```

- [ ] **Step 2: Configura Airflow connection per Spark**

Tramite UI o CLI:

```bash
docker compose exec airflow-webserver airflow connections add spark_default \
  --conn-type spark \
  --conn-host "spark://spark" \
  --conn-port 7077
```

Expected: connection creata.

- [ ] **Step 3: Carica fixture su MinIO per test**

```bash
docker compose exec minio-init mc cp /tests/fixtures/sample_pushshift.jsonl.gz \
  local/bronze-raw/sample.jsonl.gz
```

(Adatta path se necessario; eventualmente monta `tests/fixtures` come volume.)

- [ ] **Step 4: Trigger manuale del DAG**

```bash
docker compose exec airflow-webserver airflow dags trigger ingest_pushshift_batch \
  --conf '{"input_path":"s3a://bronze-raw/sample.jsonl.gz","output_path":"s3a://bronze/posts/"}'
```

Expected: task completato.

Verifica su MinIO:

```bash
docker compose exec minio-init mc ls -r local/bronze/posts/
```

Expected: file Delta parquet + `_delta_log/`.

- [ ] **Step 5: Commit**

```bash
git add airflow/dags/ingest_pushshift_dag.py
git commit -m "feat(airflow): add Pushshift batch DAG with parameterized input"
```

---

### Task 13: Integration Test End-to-End Bronze

**Files:**
- Create: `tests/integration/__init__.py`
- Create: `tests/integration/test_e2e_bronze.py`

- [ ] **Step 1: Implementa integration test**

`tests/integration/test_e2e_bronze.py`:

```python
"""End-to-end integration test for the bronze ingestion path.

Spins up Kafka + MinIO via testcontainers, publishes synthetic events
through the PrawProducer interface (with a fake PRAW), runs a single
batch of the Kafka→Bronze Spark job (trigger=availableNow), and asserts
that bronze Delta tables contain the expected records.
"""

import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from kafka import KafkaProducer
from pyspark.sql import SparkSession
from testcontainers.kafka import KafkaContainer
from testcontainers.minio import MinioContainer

from pipeline.ingestion.kafka_to_bronze import build_spark, run_stream
from pipeline.ingestion.praw_producer import PrawProducer

FIXTURE = Path(__file__).parent.parent / "fixtures" / "sample_praw_post.json"


@pytest.mark.integration
@pytest.mark.slow
def test_kafka_to_bronze_end_to_end(tmp_path):
    with KafkaContainer() as kafka, MinioContainer() as minio:
        bootstrap = kafka.get_bootstrap_server()

        # 1. Publish a fake post via PrawProducer
        producer = KafkaProducer(bootstrap_servers=bootstrap)
        fake_reddit = MagicMock()
        fake_sub = MagicMock()
        data = json.loads(FIXTURE.read_text())
        for k, v in data.items():
            setattr(fake_sub, k, v)
        fake_sub.kind = "t3"
        fake_sub.subreddit = "PokemonPlatinum"
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
```

- [ ] **Step 2: Run integration test**

```bash
uv run pytest tests/integration/test_e2e_bronze.py -v -m integration
```

Expected: 1 passed (richiede Docker attivo per testcontainers).

**Note:** primo run scarica le immagini Docker → può richiedere 2-3 minuti. Successivi run ~30s.

- [ ] **Step 3: Commit**

```bash
git add tests/integration/
git commit -m "test(integration): end-to-end Kafka→Bronze with testcontainers"
```

---

### Task 14: Terraform AWS Minimo (S3 + IAM)

**Files:**
- Create: `infra/terraform/main.tf`
- Create: `infra/terraform/variables.tf`
- Create: `infra/terraform/outputs.tf`
- Create: `infra/terraform/terraform.tfvars.example`
- Create: `infra/terraform/README.md`

- [ ] **Step 1: Crea `infra/terraform/variables.tf`**

```hcl
variable "aws_region" {
  type    = string
  default = "us-east-1"
}

variable "project_name" {
  type    = string
  default = "reddit-kg"
}

variable "env" {
  type    = string
  default = "dev"
}

variable "bronze_bucket_name" {
  type        = string
  description = "Globally-unique S3 bucket name for bronze (e.g. reddit-kg-bronze-dev-abc)"
}

variable "silver_bucket_name" {
  type = string
}

variable "gold_bucket_name" {
  type = string
}
```

- [ ] **Step 2: Crea `infra/terraform/main.tf`**

```hcl
terraform {
  required_version = ">= 1.9.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.60"
    }
  }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project = var.project_name
      Env     = var.env
      Owner   = "team"
    }
  }
}

resource "aws_s3_bucket" "bronze" {
  bucket = var.bronze_bucket_name
}

resource "aws_s3_bucket" "silver" {
  bucket = var.silver_bucket_name
}

resource "aws_s3_bucket" "gold" {
  bucket = var.gold_bucket_name
}

resource "aws_s3_bucket_versioning" "bronze_versioning" {
  bucket = aws_s3_bucket.bronze.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "bronze_lifecycle" {
  bucket = aws_s3_bucket.bronze.id

  rule {
    id     = "expire-noncurrent"
    status = "Enabled"
    noncurrent_version_expiration {
      noncurrent_days = 30
    }
  }
}

resource "aws_iam_user" "pipeline" {
  name = "${var.project_name}-${var.env}-pipeline"
}

resource "aws_iam_access_key" "pipeline" {
  user = aws_iam_user.pipeline.name
}

data "aws_iam_policy_document" "pipeline_s3" {
  statement {
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:ListBucket",
      "s3:GetBucketLocation",
    ]
    resources = [
      aws_s3_bucket.bronze.arn,
      "${aws_s3_bucket.bronze.arn}/*",
      aws_s3_bucket.silver.arn,
      "${aws_s3_bucket.silver.arn}/*",
      aws_s3_bucket.gold.arn,
      "${aws_s3_bucket.gold.arn}/*",
    ]
  }
}

resource "aws_iam_user_policy" "pipeline_s3" {
  user   = aws_iam_user.pipeline.name
  policy = data.aws_iam_policy_document.pipeline_s3.json
}
```

- [ ] **Step 3: Crea `infra/terraform/outputs.tf`**

```hcl
output "bronze_bucket" {
  value = aws_s3_bucket.bronze.bucket
}

output "silver_bucket" {
  value = aws_s3_bucket.silver.bucket
}

output "gold_bucket" {
  value = aws_s3_bucket.gold.bucket
}

output "pipeline_user_access_key" {
  value     = aws_iam_access_key.pipeline.id
  sensitive = true
}

output "pipeline_user_secret_key" {
  value     = aws_iam_access_key.pipeline.secret
  sensitive = true
}
```

- [ ] **Step 4: Crea `infra/terraform/terraform.tfvars.example`**

```hcl
# Copy to terraform.tfvars and fill in. terraform.tfvars is gitignored.

aws_region         = "eu-central-1"
project_name       = "reddit-kg"
env                = "dev"
bronze_bucket_name = "reddit-kg-bronze-dev-CHOOSE_UNIQUE_SUFFIX"
silver_bucket_name = "reddit-kg-silver-dev-CHOOSE_UNIQUE_SUFFIX"
gold_bucket_name   = "reddit-kg-gold-dev-CHOOSE_UNIQUE_SUFFIX"
```

- [ ] **Step 5: Crea `infra/terraform/README.md`**

```markdown
# Terraform AWS Infrastructure

Minimum infra for the bronze/silver/gold S3 buckets + a dedicated IAM
user with R/W permissions on those buckets only.

## Apply

```bash
cd infra/terraform
cp terraform.tfvars.example terraform.tfvars
# edit terraform.tfvars with globally-unique bucket names

terraform init
terraform plan
terraform apply
```

Take the outputs and add them to your `.env`:

```bash
terraform output -json | jq -r '.pipeline_user_access_key.value, .pipeline_user_secret_key.value'
```

## Destroy

```bash
terraform destroy
```

NOTE: this only removes infra. Bucket contents are deleted if buckets are non-empty (see `force_destroy` in the bucket resource if needed for dev).
```

- [ ] **Step 6: Verifica syntax Terraform**

```bash
cd infra/terraform
terraform init -backend=false
terraform validate
```

Expected: `Success! The configuration is valid.`

- [ ] **Step 7: Commit (NON apply, AWS credentials non disponibili in CI)**

```bash
cd ../..
git add infra/terraform/
git commit -m "infra: add Terraform for S3 buckets and IAM pipeline user"
```

---

## Sprint W1-W2 Acceptance Criteria

Prima di chiudere lo sprint, verifica:

- [ ] `uv run pytest tests/unit -v` → tutti i test unit passano.
- [ ] `uv run pytest tests/integration -v -m integration` → integration test passa.
- [ ] `docker compose up -d` in `docker/` parte senza errori, tutti gli health check verdi entro 90s.
- [ ] CLI `uv run ingest praw-fetch --subreddit PokemonPlatinum --limit 5` pubblica su Kafka (richiede `.env` con credenziali Reddit).
- [ ] Airflow UI accessibile a `http://localhost:8081` (admin/admin), entrambi i DAG presenti.
- [ ] `terraform validate` in `infra/terraform/` passa.
- [ ] `ruff check pipeline tests airflow` → 0 errori.
- [ ] Branch `sprint/w1-bootstrap` e `sprint/w2-bronze` mergiate in `main` via PR.
- [ ] Tag `v0.1.0-bronze` creato dopo merge.

```bash
git tag -a v0.1.0-bronze -m "End of W2: bronze ingestion working end-to-end"
```

---

## Next Steps (dopo W1-W2)

Una volta che lo sprint W1-W2 è completato:

1. Eseguire la skill `writing-plans` con argomento "W3 Silver Layer + Data Governance" → produrrà il prossimo piano dettagliato bite-sized.
2. Il piano W3 farà riferimento a questo documento (Part A) per allinearsi al master roadmap e alle convenzioni stabilite.
3. Ripetere il pattern per W4, W5-6, W7, W8.

---

## Self-Review Notes

Verifica spec coverage rispetto a Part A:

- ✅ Ingestion (Pushshift batch + PRAW + Kafka) — coperta in W2 Task 5-13.
- ✅ Layer bronze su Delta Lake (MinIO locale / S3 cloud) — Task 8, 9.
- ✅ Schema PySpark unificato post+commenti — Task 5.
- ✅ Metadata di ingestion (`ingest_ts`, `source`, `pipeline_version`) — Task 5.
- ✅ Test (unit + integration) — Task 5, 6, 8, 9, 13.
- ✅ Orchestrazione Airflow — Task 10, 11, 12.
- ✅ Dev env locale con docker-compose — Task 4, 10.
- ✅ Infra AWS Terraform — Task 14.
- ✅ Linting e CI prerequisites (ruff, pytest config) — Task 2.

Spec sections W3-W8 sono in Part A (master roadmap) come task-units, non come bite-sized; verranno espanse rolling.

Sezioni della spec non coperte in questo plan (correttamente, perché sono in W3+):
- Cleaning bronze→silver (W3)
- Phase 1 schema discovery (W4)
- Phase 2 teacher-student (W5-W6)
- Image pipeline (W7)
- Neo4j loading + RAG stub + validators (W8)
