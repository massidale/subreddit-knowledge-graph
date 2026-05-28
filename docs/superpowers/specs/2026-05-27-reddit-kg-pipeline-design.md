# Design Spec — Reddit → Community Discourse KG Pipeline

**Data:** 2026-05-27
**Autori:** Massimo + team (2 persone)
**Status:** Design proposto, in attesa di approvazione
**Project path:** `/Users/massimo/Documents/dev/big-data/primo-progetto/`

---

## Context — perché esiste questo progetto

Costruire una pipeline big-data **completamente automatizzata** che, dato un subreddit qualsiasi come unico input, produca un Knowledge Graph (KG) di qualità navigabile in Neo4j e pronto per essere consumato da un futuro sistema KG-RAG.

**Vincoli e obiettivi:**
- Progetto universitario di Big Data: deve dimostrare uso reale di tecnologie distribuite (Spark, Kafka, data lake) ma il **focus principale è la qualità del KG**.
- Pipeline **subreddit-agnostic**: lo stesso codice deve funzionare su r/PokemonPlatinum, r/tiramisu, r/chess. Nessuna assunzione dominio-specifica nel core.
- Dominio di test principale: **Pokémon Platinum** (subreddit di nicchia, vocabolario denso, immagini ricche, validabile esternamente).
- Risorse: AWS (~100$ totali per 2 persone). Da considerare: niente GPU 24/7, modelli leggeri quando possibile.
- Deliverable include sia processing batch (dump storico) che ingestion incrementale (API).
- KG deve essere predisposto per futura implementazione di RAG-KG (vector index, provenance, embeddings sui nodi).

**Insight chiave (pivot del design):** il valore di Reddit non sono i fatti — quelli stanno già in fonti ufficiali (PokeAPI, Wikidata) — ma il **discorso comunitario**: opinioni, strategie, tips, controversie, esperienze. Il KG che costruiamo è quindi un **Community Discourse Graph**, dove ogni triple è una **claim reificata** con attribuzione, sentiment, stance e provenance. Le entità sono ancorate a Wikidata (universale) dove possibile, ma non è obbligatorio.

---

## Approccio scelto — Approccio A: Medallion Lakehouse + Kafka di staging

Pipeline lakehouse classica in tre layer (bronze/silver/gold) su S3+Delta Lake, processing Spark su EMR, orchestrazione Airflow, Kafka come buffer di staging per l'ingestion incrementale (non real-time). Storage finale Neo4j Community.

Approccio scartato: Spark-only senza Kafka (più semplice ma meno dimostrativo del pattern producer/consumer richiesto in un corso big-data, e meno estendibile per il RAG futuro).

---

## Architettura ad alto livello

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        AIRFLOW (orchestratore)                          │
└─────────────────────────────────────────────────────────────────────────┘
       │              │              │              │              │
       ▼              ▼              ▼              ▼              ▼
┌─────────────┐ ┌──────────┐ ┌────────────┐ ┌────────────┐ ┌──────────────┐
│ PRAW poller │ │ Pushshift│ │ Spark      │ │ Phase-1    │ │ Phase-2      │
│ (incre-     │ │ loader   │ │ cleaning + │ │ Schema     │ │ Bulk extract │
│  mental)    │ │ (batch)  │ │ governance │ │ Discovery  │ │ + load Neo4j │
└──────┬──────┘ └────┬─────┘ └──────┬─────┘ └─────┬──────┘ └──────┬───────┘
       │             │              │             │              │
       ▼             │              ▼             ▼              ▼
   ┌────────┐        │       ┌───────────────────────────────────────┐
   │ Kafka  │────────┘       │      S3 + Delta Lake (medallion)      │
   │(buffer)│                │  bronze ──▶ silver ──▶ gold           │
   └────────┘                └───────────────────────────────────────┘
                                                          │
                                                          ▼
                                                   ┌──────────────┐
                                                   │   Neo4j KG   │
                                                   │ (+ embed.)   │
                                                   └──────┬───────┘
                                                          │
                                                          ▼ (futuro)
                                                   ┌──────────────┐
                                                   │  KG-RAG API  │
                                                   └──────────────┘
```

### Componenti

| Componente | Ruolo | Tech |
|------------|-------|------|
| **PRAW poller** | Polling periodico Reddit API, produce su Kafka topic `reddit-raw` | Python + PRAW su EC2 t3.small |
| **Pushshift loader** | Carica dump storico una-tantum su bronze | Spark job |
| **Kafka** | Buffer di staging per gli incrementali (no real-time, finestre da minuti) | Self-hosted EC2 (alternativa MSK Serverless) |
| **Spark cleaning** | Bronze → Silver: dedup, schema validation, deidentificazione, lingua | EMR cluster CPU |
| **Phase 1 (Schema Discovery)** | Sample ~500-1000 post → 1 chiamata LLM → schema JSON versionato | Job single-shot + API Claude Haiku 4.5 |
| **Phase 2 (Bulk Extraction)** | Teacher-Student: LLM su sample + GLiNER/REBEL/heuristic sul resto | EMR + 1× g4dn.xlarge spot |
| **Sentiment & Stance** | Classificazione di ogni claim estratta | HuggingFace su Spark CPU |
| **Canonicalization & Anchoring** | DBSCAN su embedding entità + linking opzionale a Wikidata | Spark + SPARQL Wikidata |
| **Neo4j loader** | Scrive nodi/edge + embedding properties | `neo4j-spark-connector` |
| **Airflow** | DAG che coordina l'intera pipeline | EC2 t3.small o docker-compose locale |
| **Image pipeline** | Download → dedup phash → CLIP embedding → link a entità | Spark + 1× g4dn.xlarge spot |

---

## Data flow medallion + Data Governance

### Bronze (raw, immutable)
- Pushshift JSON gzippato → S3 in Parquet partizionato per `subreddit`/`created_date`.
- Reddit API events → Kafka → Spark Structured Streaming → stesso prefisso S3.
- **Nessuna trasformazione**, solo serializzazione e metadati di ingestion: `ingest_ts`, `source`, `pipeline_version`.

### Silver (cleaned & governed)
1. **Schema enforcement** via Delta Lake constraints (tipi forti, NOT NULL su `id`, `created_utc`).
2. **Deduplication** su `post_id` (Pushshift e API si sovrappongono).
3. **Quality checks** con Great Expectations: `body` non vuoto, validità timestamp, length min, language detection (`fasttext-langid`); mantieni solo `en`/`it` (Pokémon Platinum è prevalentemente en).
4. **Deidentificazione**: hash SHA-256 + salt sui `username` (privacy/governance soft). Mention `u/xxx` riconoscibili ma non re-identificabili.
5. **Normalizzazione testuale**: rimozione URL/markdown/emoji artifacts; conserva testo originale in colonna separata `body_raw`.
6. **Filtri qualità**: rimuovi `[deleted]`/`[removed]`, score ≤ 0 e < 10 char.

### Gold (KG-ready)
Tabelle Delta versionate (time-travel per riproducibilità):
- `posts_enriched(post_id, body, embedding_384, lang, score, parent_id, ...)`
- `entities_extracted(entity_id, canonical_id, type, name, embedding_384, wikidata_qid?, post_ids[])`
- `claims_extracted(claim_id, head_entity_id, rel_type, tail_entity_id, asserter_hash, score, sentiment, stance, evidence_snippet, source_post_id, ...)`
- `images_indexed(phash, s3_url, embedding_clip_512, post_ids[])`
- `claim_aggregates(head, rel, tail, n_supports, n_contests, status: CONSENSUS|CONTESTED|NICHE)`

**Lineage**: tag su Delta commit + Airflow XCom per tracciare quale `pipeline_version` e quale `domain_schema_version` hanno prodotto ogni record.

---

## KG Phase 1 — Schema/Domain Discovery (agnostico)

**Obiettivo:** dare a un LLM potente un campione piccolo ma di alta qualità per scoprire automaticamente la "forma" del dominio, **senza alcuna assunzione dominio-specifica nel prompt**.

### Step
1. **Campionamento stratificato** dalla silver layer:
   - 60% post con score nel top 1%
   - 30% post lunghi diversificati per `flair`/topic-cluster
   - 10% post recenti per termini emergenti
2. **Prompt agnostico** (funziona identico per qualsiasi subreddit):

```
You are a knowledge graph engineer. Your task is to discover the
semantic domain of a community from a sample of its discussions.

Read the following N posts from an online community. WITHOUT assuming
any prior knowledge of the topic:

1. Identify recurring CONCEPT CLASSES (entity types). For each:
   - Generic name (PascalCase)
   - 1-line definition
   - 3-5 example instances found in the text
   - Optional alias patterns observed

2. Identify recurring RELATIONSHIP PATTERNS. For each:
   - Verb-style name (UPPER_SNAKE)
   - Head class → Tail class
   - 2-3 example sentences expressing it

3. Dominant LANGUAGE and REGISTER.

Be exhaustive but DO NOT invent classes with < 3 occurrences.
Output STRICT JSON per the provided schema. No prose.
```

3. **Output**: `domain_schema_vN.json` versionato in S3 + Git.
   ```json
   {
     "subreddit": "<name>",
     "discovered_at": "...",
     "language": "en",
     "entity_types": [
       {"name": "Concept", "definition": "...", "examples": [...], "alias_patterns": [...]}
     ],
     "relation_types": [
       {"name": "VERB_FORM", "head": "ClassA", "tail": "ClassB", "examples": [...]}
     ]
   }
   ```
4. **Human-in-the-loop opzionale**: review/edit del JSON prima della Fase 2 (5 minuti). Vale oro.

---

## KG Phase 2 — Bulk Extraction (Teacher-Student) + Discourse Reification

### Pivot — ogni triple è una *Claim*, non un fatto

Il KG non rappresenta verità assolute. Ogni triple estratta è una **claim** asserita da uno (o più) utenti Reddit con metadati di provenienza, sentiment e stance.

### Strategia "teacher-student" per non bruciare il budget LLM

**Teacher (LLM, su sample di 5-10K post):**
- LangChain `LLMGraphTransformer` con `allowed_nodes` = `entity_types` dello schema e `allowed_relationships` = `relation_types`.
- LLM: Claude Haiku 4.5 (basso costo).
- Output: triple di alta qualità sul campione.

**Student (modelli leggeri, su tutto il resto):**
- **NER**: GLiNER (`urchade/gliner_medium-v2.1`, ~200MB, zero-shot) con `entity_types` dello schema. Distribuito Spark con `mapPartitions` (1 modello per executor).
- **RE**: due opzioni in parallelo:
  - **REBEL** (`Babelscape/rebel-large`) filtrato per matching schema (GPU spot ~1h).
  - **Heuristica** co-occurrence + dependency parsing spaCy (fallback CPU gratis).
- Confidence combinata: `entity_conf × relation_conf × extractor_weight`.

**Merge:** triple del Teacher hanno priorità + boost confidence; le Student integrano e ampliano la copertura.

### Sentiment & Stance per ogni claim
- **Sentiment**: `cardiffnlp/twitter-roberta-base-sentiment-latest`, ~500MB, su Spark CPU.
- **Stance**: NLI model `MoritzLaurer/DeBERTa-v3-base-mnli-fever` → classifica se la frase **supporta** / **contraddice** / è **neutrale** rispetto alla triple. Output: stance ∈ {support, contest, neutral}.

### Canonicalization & Anchoring (universale, Wikidata)
1. **Embedding entità** con sentence-transformers MiniLM-L6 (384d).
2. **DBSCAN** sugli embedding per cluster di alias → assegna `canonical_id` locale.
3. **Wikidata linking** opzionale:
   - Per ogni canonical entity → SPARQL `?item rdfs:label "<name>"@<lang>` (cache per ridurre query).
   - Se match → salva `wikidata_qid`. Se no match, l'entità rimane subreddit-native.
   - Molte entità *discourse-specific* (es. "the cynthia fight strategy") **non** devono avere Q-ID.
4. **NO PokeAPI nel core**. Domain-specific anchoring vive come plug-in opzionale (vedi sotto).

### Aggregazione claims → relazioni semantiche
Dopo l'estrazione, un job aggrega claim sulla stessa coppia (head, rel, tail):
- `CONSENSUS` se molti utenti dicono la stessa cosa con score alto
- `CONTESTED` se opinion divergenti con stance opposte significative
- `NICHE` se claim singola ma di alto score

Property aggregata sulla relazione finale in Neo4j.

### Load Neo4j
- Nodi: `(:Entity {canonical_id, type, name, embedding, wikidata_qid?})`, `(:Image {phash, s3_url, embedding})`, `(:Post {id, score, ...})`.
- Edge: `(:Entity)-[:REL_TYPE {n_supports, n_contests, status, source_post_ids[], avg_sentiment}]->(:Entity)`.
- Edge `(:Entity)-[:DEPICTED_BY {clip_similarity}]->(:Image)`.
- Edge `(:Entity)-[:MENTIONED_IN]->(:Post)`.
- **Vector index** su `:Post.embedding`, `:Entity.embedding`, `:Image.embedding` per RAG futuro.

### Plug-in di validazione dominio-specifica (showcase, fuori dal core)
- `validators/pokeapi_demo.ipynb` — notebook che, **a valle** della pipeline, cross-checka le entità `Pokemon`, `Move`, `Ability` estratte contro PokeAPI per metrics di precision/recall **nel report**.
- Per altri dominî lo slot è popolabile (es. `validators/chesscom_demo.ipynb`). Il core non chiama mai questi validatori.

---

## Pipeline Immagini

### V1 (deliverable): solo embedding, no captioning
Su Reddit l'immagine è già contestualizzata dal testo dell'utente, quindi il captioning è in larga parte ridondante. Saltarlo non è una perdita reale, e fa risparmiare GPU.

### Step
1. Estrai `image_url` da silver layer (regex Reddit / imgur / native gallery).
2. **Spark job `images_download`**: download asincrono con backoff/timeout.
3. **Dedup per perceptual hash** (`imagehash.phash`) **prima** dell'embedding. Reddit ha tantissimi repost, dimezza la GPU time.
4. **Embedding CLIP** (`openai/clip-vit-base-patch32`, 350MB) su 1× g4dn.xlarge spot, batch 64.
5. **Aggancio al KG via CLIP text-image** (elegante: CLIP è multimodale):
   - Per ogni canonical entity testuale, embedda il nome con CLIP text encoder.
   - Top-K cosine similarity vs CLIP image embeddings (threshold ~0.28).
   - Crea edge `(:Entity)-[:DEPICTED_BY {score}]->(:Image)`.

### V2 (documentato, fuori scope progetto)
Interface `ImageProcessor.process(image) -> (embedding, caption?)`. V1 implementa solo embedding. V2 si aggancia con BLIP-2 se future risorse lo permettono.

---

## RAG-readiness — cosa esce già pronto

Il primo progetto **non implementa il RAG**, ma la pipeline produce tutto il necessario:

1. **Embeddings universali** sui nodi e sui post (vector index nativi Neo4j 5.x).
2. **Provenance citation-ready**: ogni edge ha `source_post_ids[]` ed `evidence_snippet`.
3. **Schema documentato**: `domain_schema.json` versionato → utile per generare query Cypher template via LLM nel futuro RAG.
4. **Interface `KGRetriever` documentata** (non implementata):
   ```
   class KGRetriever:
       def retrieve(question: str) -> List[ContextChunk]:
           # 1. embed question (sentence-transformers)
           # 2. vector search top-K posts (Neo4j vector index)
           # 3. for each post → graph traversal di entità correlate (1-2 hops)
           # 4. assemble context con provenance e stance
   ```

---

## Budget AWS stimato (totale ~70-95$ su 100$)

| Voce | Costo stimato |
|------|---------------|
| EMR Spark cluster (3-4 esperimenti) | 25-35$ |
| Kafka self-hosted EC2 t3.medium | 5-8$ |
| Airflow EC2 t3.small | 5-10$ |
| S3 storage (dump + medallion) | 5-10$ |
| S3 transfer + requests | 3-5$ |
| GPU spot g4dn.xlarge (~6-8h tot.) | 3-5$ |
| API LLM Phase 1 (Claude Haiku) | 1-2$ |
| API LLM Teacher Phase 2 | 5-10$ |
| Sentiment/Stance models | 0$ |
| Wikidata SPARQL | 0$ |
| Neo4j Community / Aura Free | 0$ |
| Buffer ~25% | 15-20$ |
| **TOTALE** | **70-95$** |

---

## Timeline (2 persone, 6-8 settimane part-time)

- **W1-2**: setup AWS, Terraform-light, ingestion Pushshift + PRAW + Kafka, layer bronze.
- **W3**: Spark cleaning + governance + silver. Great Expectations setup.
- **W4**: Fase 1 schema discovery + Airflow DAG end-to-end.
- **W5-6**: Fase 2 teacher-student + canonicalization + Wikidata anchoring + sentiment/stance.
- **W7**: Pipeline immagini (download, CLIP, linking).
- **W8**: Load Neo4j + queries demo + PokeAPI validator notebook + report. Buffer.

---

## Strategia di testing & validazione

### Test funzionali (CI)
- **Unit test** parser (PRAW, Pushshift) → schema interno.
- **Unit test** cleaning step (deidentificazione, language detection).
- **Property-based test** (Hypothesis) su schema enforcement Delta.
- **Integration test "tiny pipeline"**: 50 post fake → pipeline locale Docker → assert KG ha N nodi/edge.

### Validazione qualità KG (riproducibile)
- **Inter-extractor agreement** Teacher vs Student: F1 su sample comune. Target ≥ 0.7.
- **Schema adherence** Fase 2: % triple con tipi dichiarati in Fase 1. Target ≥ 95%.
- **Wikidata coverage**: % entità con Q-ID (misurato, non target).
- **Cluster purity** canonicalization: review manuale su 100 sample.

### Validazione manuale
- 200 triple random review (2 persone) con scorecard → precision umana.
- 100 immagini-entity link review → similarity CLIP fa senso?

### Validation dominio-specifica (showcase)
- `validators/pokeapi_demo.ipynb`: cross-check entità Pokemon estratte vs PokeAPI per il report.

### Discourse Graph sanity check
- Esaminare manualmente claim CONSENSUS e CONTESTED — sono significative?
- Bias check sulla sentiment classification (es. negation handling).

### End-to-end demo
- Notebook `from_zero_to_kg.ipynb`: input = subreddit name → pipeline ridotta → KG.
- Replicabile su un secondo subreddit (es. r/chess) per dimostrare agnosticità.
- Query Cypher di esempio + screenshot Neo4j Browser nel report.

---

## File critici (paths da creare durante l'implementazione)

```
primo-progetto/
├── infra/                       # Terraform AWS (EMR, S3, EC2, MSK opzionale)
├── airflow/dags/                # DAG di orchestrazione
│   ├── ingest_pushshift_dag.py
│   ├── ingest_praw_streaming_dag.py
│   ├── bronze_to_silver_dag.py
│   ├── phase1_schema_discovery_dag.py
│   ├── phase2_extraction_dag.py
│   └── load_neo4j_dag.py
├── pipeline/
│   ├── ingestion/
│   │   ├── praw_producer.py
│   │   ├── pushshift_loader.py
│   │   └── kafka_to_bronze.py
│   ├── cleaning/
│   │   ├── deduplication.py
│   │   ├── deidentification.py
│   │   └── quality_gx.py        # Great Expectations
│   ├── phase1/
│   │   ├── sampler.py
│   │   ├── schema_discovery.py
│   │   └── schema_validator.py
│   ├── phase2/
│   │   ├── teacher_llm_extract.py
│   │   ├── student_gliner.py
│   │   ├── student_rebel.py
│   │   ├── student_heuristics.py
│   │   ├── sentiment_stance.py
│   │   ├── canonicalization.py
│   │   ├── wikidata_linker.py
│   │   └── claim_aggregator.py
│   ├── images/
│   │   ├── downloader.py
│   │   ├── phash_dedup.py
│   │   ├── clip_embedder.py
│   │   └── entity_image_linker.py
│   └── neo4j_loader/
│       └── load_kg.py
├── validators/
│   ├── pokeapi_demo.ipynb       # plug-in showcase, non in pipeline
│   └── chesscom_demo.ipynb      # placeholder per agnostic demo
├── rag/                          # interface stub, no implementation
│   └── kg_retriever.py
├── notebooks/
│   ├── from_zero_to_kg.ipynb
│   └── eval_kg_quality.ipynb
├── tests/
│   ├── unit/
│   ├── property/
│   └── integration/
├── docker/                       # docker-compose dev environment
├── requirements.txt
└── README.md
```

---

## Strumenti/librerie open-source riutilizzati

- **Ingestion**: PRAW, pushshift dumps
- **Distributed**: Apache Spark (PySpark), Kafka, Delta Lake
- **Orchestration**: Apache Airflow
- **Data governance**: Great Expectations, Delta Lake constraints, `fasttext-langid`
- **NLP Phase 1**: Claude Haiku via API (`anthropic` SDK)
- **NLP Phase 2 Teacher**: LangChain `LLMGraphTransformer` (`langchain-experimental`)
- **NLP Phase 2 Student**: GLiNER (`gliner`), REBEL (HuggingFace `transformers`), spaCy
- **Sentiment**: `cardiffnlp/twitter-roberta-base-sentiment-latest`
- **Stance**: `MoritzLaurer/DeBERTa-v3-base-mnli-fever`
- **Embeddings testo**: `sentence-transformers/all-MiniLM-L6-v2`
- **Image dedup**: `imagehash` (phash)
- **Image embeddings**: `openai/clip-vit-base-patch32` (via `transformers`)
- **Anchoring**: SPARQL Wikidata (`SPARQLWrapper`), opzionale BLINK
- **Graph DB**: Neo4j Community + `neo4j-spark-connector` o `neo4j-graphrag-python`

---

## Verifica end-to-end

Per validare che la pipeline funzioni davvero "da subreddit a grafo":

1. **Run completo**: lanciare Airflow DAG `master_pipeline` con parametro `subreddit=PokemonPlatinum`.
2. **Sanity checks automatici**:
   - Bronze conta ≥ 100K post.
   - Silver scarta ≥ 10% per quality (deleted/removed/score≤0).
   - Gold ha entities ≥ 1000 e claims ≥ 5000.
   - Neo4j vector index attivi su Post/Entity/Image.
3. **Query Cypher demo** (da includere nel report):
   - "Top 10 strategie per Cynthia (consensus)"
   - "Pokémon più contestati"
   - "Workaround più upvotati per ROM hack su Platinum"
4. **Agnosticità**: rilanciare la stessa pipeline su un secondo subreddit (r/chess) e mostrare il KG con schema diverso ma stesso codice.
5. **Plug-in validator**: aprire `validators/pokeapi_demo.ipynb` e mostrare le metriche di precision/recall.

---

## Aspetti che restano da decidere in fase di implementazione

- Numero esatto di subreddit secondari da processare (dipende da budget residuo).
- Scelta finale Neo4j Community self-hosted vs Aura Free (vincoli quota nodi/edge).
- Kafka MSK vs self-hosted (semplicità vs costo).
- Topic modeling BERTopic come arricchimento opzionale (skipped nel design base, valutare in W5).

---

## Hand-off al writing-plans

Dopo approvazione di questa spec, lo step successivo (uscendo dal plan mode) è invocare la skill `writing-plans` per produrre un piano di implementazione settimanale dettagliato che mappi questa spec in task settimanali eseguibili dai 2 membri del team in parallelo.
