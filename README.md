# airflow-etl-music-features

[![CI](https://github.com/soyDCAR/airflow-etl-music-features/actions/workflows/ci.yml/badge.svg)](https://github.com/soyDCAR/airflow-etl-music-features/actions/workflows/ci.yml)
[![Python 3.11](https://img.shields.io/badge/python-3.11-blue.svg)](https://www.python.org/downloads/release/python-3110/)
[![Airflow 2.9](https://img.shields.io/badge/airflow-2.9-017CEE?logo=apacheairflow&logoColor=white)](https://airflow.apache.org/)
[![Docker](https://img.shields.io/badge/docker-compose-2496ED?logo=docker&logoColor=white)](https://docs.docker.com/compose/)
[![Postgres 15](https://img.shields.io/badge/postgres-15-336791?logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)

Production-grade ETL pipeline that ingests [Free Music Archive (FMA)](https://github.com/mdeff/fma) audio samples, extracts acoustic features with **librosa**, and loads them into a **Postgres** data warehouse — all orchestrated by **Apache Airflow** inside **Docker Compose**.

---

## Pipeline Architecture

```mermaid
flowchart LR
    subgraph sources["Data Sources"]
        FMA["FMA Archive\n(real MP3s)\nor Synthetic\nsignals"]
    end

    subgraph airflow["Apache Airflow 2.9 — LocalExecutor"]
        T1["download_fma_sample\n📥 Stage tracks"]
        T2["extract_features\n🎵 MFCC · centroid · tempo"]
        T3["load_to_postgres\n🗄️ Upsert warehouse"]
        T1 --> T2 --> T3
    end

    subgraph docker["Docker Compose"]
        airflow
        WH[("postgres-warehouse\nmusic_features DB\n:5433")]
        META[("postgres-airflow\nmetadata DB")]
    end

    FMA --> T1
    T3 --> WH
    airflow -.->|"DAG state\nXCom / logs"| META
```

---

## Quickstart

### 1. Clone & configure

```bash
git clone https://github.com/soyDCAR/airflow-etl-music-features.git
cd airflow-etl-music-features

cp .env.example .env
# Edit .env — generate a Fernet key:
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
# Paste the output as FERNET_KEY in .env
```

### 2. Build & start

```bash
# First run: initialises metadata DB and creates the admin user
docker compose up airflow-init

# Then bring up the full stack
docker compose up --build -d
```

### 3. Open the UI

Navigate to [http://localhost:8080](http://localhost:8080) — credentials are `airflow / airflow` (or whatever you set in `.env`).

Enable and trigger `extract_fma_features`. By default it runs in **synthetic mode** (no downloads, instant results). To use real FMA data set the Airflow Variable `FMA_USE_SYNTHETIC` to `false`.

---

## Warehouse Schema

**Table:** `audio_features`

| Column | Type | Description |
|---|---|---|
| `id` | `BIGSERIAL` | Auto PK |
| `track_id` | `VARCHAR(20)` | FMA track ID or `SYN_XXXX` |
| `title` | `TEXT` | Track title |
| `duration_sec` | `FLOAT` | Clip duration in seconds |
| `sample_rate` | `INTEGER` | Audio sample rate (Hz) |
| `mfcc_mean` | `JSONB` | Mean of 13 MFCC coefficients |
| `mfcc_std` | `JSONB` | Std dev of 13 MFCC coefficients |
| `spectral_centroid_mean` | `FLOAT` | Mean spectral centroid (Hz) |
| `spectral_centroid_std` | `FLOAT` | Std dev spectral centroid |
| `tempo` | `FLOAT` | Estimated BPM |
| `processed_at` | `TIMESTAMPTZ` | Last pipeline run time |
| `created_at` | `TIMESTAMPTZ` | Row creation time |

Connect directly to the warehouse on `localhost:5433` (user/pass from `.env`).

---

## Development

```bash
# Lint
ruff check .
black --check .

# Tests (no Docker needed)
pip install apache-airflow==2.9.0 \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.0/constraints-3.11.txt"
pip install librosa soundfile psycopg2-binary pandas pytest ruff black

AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=sqlite:////tmp/af.db \
AIRFLOW__CORE__LOAD_EXAMPLES=False \
airflow db migrate && pytest tests/ -v
```

---

## Project Structure

```
.
├── dags/
│   └── extract_fma_features.py   # Main pipeline DAG (TaskFlow API)
├── plugins/                       # Custom Airflow operators/hooks (future)
├── tests/
│   ├── conftest.py                # Airflow env vars for CI
│   └── test_dag_integrity.py      # Structure & dependency checks
├── sql/
│   └── init/
│       └── 01_create_tables.sql   # Warehouse DDL (auto-run on first start)
├── .github/workflows/ci.yml       # Ruff + Black + pytest on push/PR
├── docker-compose.yml
├── Dockerfile                     # Extends official Airflow image + librosa
├── pyproject.toml                 # Ruff + Black + pytest config
├── requirements.txt
└── .env.example
```

---

## What's Next (P2)

| Feature | Description |
|---|---|
| **Kafka real-time ingestion** | Stream new track events instead of batch daily runs |
| **dbt transformations** | Build `dim_tracks`, `fct_audio_features`, genre aggregates |
| **Retries & alerting** | Slack callbacks on task failure, dead-letter queue |
| **Partitioned loads** | Partition `audio_features` by `processed_at` month |
| **Streamlit dashboard** | Interactive explorer for tempo, MFCC clusters, genre search |
| **Great Expectations** | Data quality checks between extract and load |

---

> **Screenshot:** *(Add Airflow UI screenshot here after first successful run)*
