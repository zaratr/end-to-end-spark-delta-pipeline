# Iceberg Zero-ETL & Spark Data Pipeline

A medallion data pipeline (Bronze → Silver) on **Apache Iceberg** with a
pluggable AI parser that turns messy unstructured logs into structured,
queryable records. Built to demonstrate Zero-ETL: raw bytes land in an
Iceberg table untouched, and the structured interpretation is
materialised in a second Iceberg table any engine (Spark, Athena,
Snowflake, Trino) can query in place — no intermediate copy.

The AI parse step uses **DSPy** with a local **Ollama (Gemma)** model.
A deterministic regex parser is the default so the pipeline runs on any
laptop without a GPU; the DSPy path is a drop-in.

**Skills demonstrated:** Apache Iceberg · medallion architecture ·
Zero-ETL · PyArrow · DSPy · Ollama/Gemma · Python · idempotent pipeline
design

---

## Architecture

```
raw logs (.jsonl / .txt)
        │  pipeline.py ingest
        ▼
┌───────────────────────────┐
│  Bronze Iceberg table     │   raw_text + provenance, never mutated
│  logging.raw_logs         │
└───────────────────────────┘
        │  pipeline.py run  (LogParser.parse_batch)
        │     ├─ RegexParser  (default, no deps)
        │     └─ DspyParser   (DSPy + Ollama Gemma)
        ▼
┌───────────────────────────┐
│  Silver Iceberg table     │   timestamp/level/service/message
│  logging.structured_logs  │   queryable by any Iceberg engine
└───────────────────────────┘
```

The bronze→silver transform is **idempotent**: only Bronze ids not
already present in Silver are processed, so the pipeline is safe to
re-run as new data lands.

---

## Why Iceberg + Zero-ETL

* **Iceberg** is an open table format: a single set of Parquet + metadata
  files is queryable by Spark, Trino, Athena, Snowflake, etc. without
  copying data between engines.
* **Zero-ETL** means the transform is a *materialisation*, not a
  pipeline of point-to-point copies. Silver is derived from Bronze in
  place; consumers read Silver directly.
* **AI in the loop:** the unstructured→structured step that used to need
  hand-written regex per log format is done by a local LLM (DSPy/Gemma),
  with a deterministic fallback so the contract holds even without a
  model.

---

## Quickstart

### 1. Install

```bash
pip install -r requirements.txt
```

For the LLM parse path additionally install DSPy and run Ollama:

```bash
pip install dspy-ai
ollama pull gemma
ollama serve   # http://localhost:11434
```

### 2. Init the warehouse (local SQLite catalog + Iceberg tables)

```bash
python pipeline.py init
```

Creates `./catalog.db` (SQL catalog) and `./warehouse/` (Parquet data).

### 3. Ingest raw logs

```bash
python pipeline.py ingest data/sample_logs.jsonl --source sample
```

Accepts `.jsonl` (`{"raw": "..."}`) or plain `.txt` (one log per line).

### 4. Run the parse step

```bash
python pipeline.py run                       # regex parser (no model)
python pipeline.py run --parser dspy         # DSPy + Ollama Gemma
```

### 5. Inspect

```bash
python pipeline.py show bronze --limit 10
python pipeline.py show silver --limit 10
```

---

## Configuration

| flag | default | purpose |
|---|---|---|
| `--catalog` | `./catalog.db` | SQLite catalog DB URI |
| `--warehouse` | `./warehouse` | filesystem dir for Iceberg data files |

For a multi-engine deployment, swap the local SQL catalog for a REST or
Nessie catalog and point the warehouse at object storage (S3/ADLS/GCS).
The pipeline code is unchanged.

---

## Repository layout

```
end-to-end-spark-delta-pipeline/
├── pipeline.py              # CLI: init / ingest / run / show
├── src/
│   ├── catalog.py           # local SQL-catalogued Iceberg warehouse
│   ├── schemas.py           # Arrow schemas for bronze + silver
│   ├── parsers.py           # LogParser ABC + Regex + DSPy(lazy)
│   ├── dspy_parser.py       # DSPy signature + Ollama/Gemma parse_log
│   └── pipeline.py          # ingest_raw / run_pipeline / read_*
├── data/sample_logs.jsonl
├── tests/test_pipeline.py   # Iceberg e2e + idempotency (no Ollama)
└── requirements.txt
```

## Running tests

```bash
pip install -r requirements.txt
pytest
```

Tests exercise table creation, bronze ingest, the bronze→silver
transform, incremental processing, and idempotency — all against a temp
SQLite/Iceberg warehouse and the regex parser, so no model is required.

## Design scope

The local warehouse uses a SQL catalog + filesystem storage so the flow
is reproducible on one machine. Production extensions (documented, not
built): REST/Nessie catalog, S3/ADLS storage, Spark Structured Streaming
for continuous ingest, schema evolution + partition specs, and a
scheduler (Airflow/Dagster) to trigger `run` on a cadence. The
bronze/silver contract and the parser abstraction are stable; those
extensions swap in behind them without changing the CLI.
