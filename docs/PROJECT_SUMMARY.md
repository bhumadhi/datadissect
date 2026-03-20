# Healthcare Data Platform — Project Summary

## Project Goal
Build a production-grade healthcare claims data pipeline end-to-end using industry standard tools — to learn the full data engineering stack independently and build a portfolio piece that demonstrates end-to-end ownership for principal/architect-level roles.

**Domain:** Healthcare claims & billing (X12 EDI 837P/837I format)
**Pattern:** Micro-batch (sweet spot for claims data)
**Philosophy:** Every design decision is conscious and explainable — not just "it works"

---

## Hardware & Environment

```
Machine:   Mac Mini M4 2024
RAM:       16GB
Storage:   512GB SSD
OS:        macOS 26 (Apple Silicon arm64)
```

**Python Environment:**
```
.venv inside project folder
Python 3.9.6
PySpark 3.5.1          ← pinned (Airflow default installs 4.x which breaks)
delta-spark 3.0.0
minio 7.2.5
psycopg2-binary 2.9.9
watchdog               ← file watcher
requests               ← Airflow REST API trigger
streamlit              ← dashboard
trino                  ← Trino Python connector
plotly                 ← charts
```

---

## Tech Stack

| Layer | Tool | Version | Why |
|---|---|---|---|
| Storage | MinIO | latest | S3-compatible, lightweight HDFS replacement |
| Processing | Apache Spark | 3.5.1 | Same as production, local mode |
| Table Format | Delta Lake | 3.0.0 | ACID, time travel, schema enforcement |
| Orchestration | Apache Airflow | 2.9.1 | Industry standard pipeline scheduler |
| Metadata | PostgreSQL | 16 | Airflow DB + pipeline audit + file registry |
| Query Engine | Trino | 435 | Cross-source SQL on Delta + PostgreSQL |
| Frontend | Streamlit | latest | Operational monitor + claims analytics |
| Containerization | Docker Desktop | - | 8GB memory limit (half of 16GB) |
| Version Control | Git | - | Every change tracked from day one |

---

## Project Structure (Current)

```
healthcare-pipeline/
├── ingestion/                    ← Kafka producer (future)
├── processing/
│   ├── scala/                    ← Scala Spark jobs (future)
│   ├── pyspark/
│   │   ├── claims_cleanse.py     ← Stage 1: cleanse ✅
│   │   ├── claims_transform.py   ← Stage 2: transform ✅
│   │   └── claims_curate.py      ← Stage 3: curate ✅
│   └── common/
│       └── path_utils.py         ← FileMeta, parse_filename(), all path functions ✅
├── orchestration/
│   └── claims_pipeline_dag.py    ← 7-task Airflow DAG ✅
├── infra/
│   ├── airflow/
│   │   └── Dockerfile            ← Custom Airflow image ✅
│   └── trino/
│       ├── config.properties     ← Trino server config ✅
│       ├── node.properties
│       ├── jvm.config
│       └── catalog/
│           ├── delta.properties      ← Delta → MinIO ✅
│           └── postgresql.properties ← PostgreSQL → pipeline_db ✅
├── frontend/
│   ├── app.py                    ← Streamlit home page ✅
│   └── pages/
│       ├── 01_pipeline_monitor.py   ← Operational view ✅
│       └── 02_claims_analytics.py   ← Claims analytics ✅
├── config/
│   └── clients/                  ← Per-client YAML configs (Phase 8)
├── scripts/
│   ├── file_watcher.py           ← Auto-trigger on file drop ✅
│   └── recover_unprocessed.py    ← CLI recovery tool ✅
├── docs/
│   └── PROJECT_SUMMARY.md
├── tests/
├── docker-compose.yml            ← Full stack ✅
├── .env                          ← Credentials (gitignored)
├── .env.example                  ← Placeholder for repo
├── .gitignore
├── requirements.txt
└── data/
    ├── inbound/    ← Drop files here — watcher monitors this
    ├── outbound/   ← Files going back to clients
    ├── hold/       ← Fallback for invalid files if MinIO unreachable
    └── reference/
        ├── cpt_codes.csv
        └── icd10_codes.csv
```

---

## Docker Services

```
Service              Port    Status
─────────────────    ────    ──────
MinIO                9001    ✅ healthy
PostgreSQL           5432    ✅ healthy
Spark                8080    ✅ running
Airflow Webserver    8082    ✅ running
Airflow Scheduler    -       ✅ running
Trino                8083    ✅ running
Streamlit            8501    ✅ running
```

---

## Service Credentials

```
MinIO Console:   http://localhost:9001   minioadmin / minioadmin123
MinIO API:       http://localhost:9000
PostgreSQL:      localhost:5432          pgadmin / pgpassword123
Airflow UI:      http://localhost:8082   admin / admin123
Spark UI:        http://localhost:8080
Trino:           http://localhost:8083
Streamlit:       http://localhost:8501
```

All credentials stored in `.env`, loaded via `os.getenv()` with fallback defaults in code.

---

## Filename Convention

```
{CLIENT_CODE}_{FILE_TYPE}_{ENV}_{DATE}_{SEQUENCE}.csv
BCBS001_837P_PROD_20260312_001.csv
```

| Part | Rule | Valid Values | Example |
|---|---|---|---|
| CLIENT_CODE | Uppercase alphanumeric, 3-10 chars | Any | BCBS001 |
| FILE_TYPE | Fixed set | 837P, 837I, 835, MEMBER, PHARMACY, CROSSWALK | 837P |
| ENV | Fixed set | PROD, TEST, SYS1, SYS2, UAT1, UAT2 | PROD |
| DATE | YYYYMMDD, real calendar date | Any valid date | 20260312 |
| SEQUENCE | 3-digit zero-padded | 001-999 | 001 |

**Why ENV in filename:** Clients send via SFTP/AS2 and can't control folder structure. The file must be self-describing.

**Why SEQUENCE:** Handles same-day re-sends and file splits without overwriting. A re-send is `_002` not a duplicate of `_001` — idempotency by convention before code.

---

## MinIO Zone Map

```
healthcare-raw/
  {CLIENT}/{FILE_TYPE}/{ENV}/{DATE}_{SEQ}/{filename}
  ← immutable by convention, system of record

healthcare-cleansed/
  claims/{CLIENT}/{FILE_TYPE}/{ENV}/{DATE}_{SEQ}/
  ← validated, deduped, PHI masked — Parquet

healthcare-quarantine/
  claims/{CLIENT}/{FILE_TYPE}/{ENV}/{DATE}_{SEQ}/  ← failed validation records
  hold/{timestamp}_{filename}                       ← invalid filename convention

healthcare-transformed/
  claims/{CLIENT}/{FILE_TYPE}/{ENV}/{DATE}_{SEQ}/  ← enriched, Delta
  summaries/payer/{CLIENT}/{FILE_TYPE}/{ENV}/{DATE}_{SEQ}/
  summaries/cpt/{CLIENT}/{FILE_TYPE}/{ENV}/{DATE}_{SEQ}/

healthcare-curated/
  member_summary/{CLIENT}/{FILE_TYPE}/{ENV}/{DATE}_{SEQ}/   ← Delta
  payer_summary/{CLIENT}/{FILE_TYPE}/{ENV}/{DATE}_{SEQ}/    ← Delta
  provider_summary/{CLIENT}/{FILE_TYPE}/{ENV}/{DATE}_{SEQ}/ ← Delta

healthcare-reference/
  cpt_codes.csv
  icd10_codes.csv

healthcare-scratch/
  {username}/{label}/    ← temp workspace, 7-day TTL lifecycle policy

healthcare-metadata/
  trino-catalog/         ← Trino file metastore
```

**Why partitioned paths:** Spark predicate pushdown — reading all BCBS001 professional claims across dates requires scanning only `claims/BCBS001/837P/PROD/*/` not the entire bucket.

**Why Parquet at cleansed, Delta at transform:** Cleansed is ephemeral staging — no version history needed. Delta starts at transform where data has business value worth protecting: ACID transactions, time travel, schema enforcement.

---

## PostgreSQL Schema

### Databases
```
pipeline_db   ← pipeline metadata and audit
airflow_db    ← Airflow internals
reference_db  ← lookup tables (future)
```

### pipeline_db Tables

#### source_system
```sql
source_id   SERIAL PRIMARY KEY,
source_name VARCHAR(100) NOT NULL UNIQUE,
source_type VARCHAR(50),
active_flag BOOLEAN NOT NULL DEFAULT TRUE,
created_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
```

#### file_registry
```sql
file_id          BIGSERIAL PRIMARY KEY,
source_id        INT REFERENCES source_system(source_id),
file_name        VARCHAR(255) NOT NULL,
bucket_name      VARCHAR(100) NOT NULL,
object_key       VARCHAR(500) NOT NULL,
file_size_bytes  BIGINT,
file_hash        VARCHAR(128),
ingestion_status VARCHAR(50) NOT NULL DEFAULT 'RECEIVED',
received_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
processed_at     TIMESTAMP,
source_name      VARCHAR(100),
error_message    TEXT,
client_code      VARCHAR(10),    ← added Phase 4
file_type        VARCHAR(20),    ← added Phase 4
env              VARCHAR(10),    ← added Phase 4
sequence         VARCHAR(3),     ← added Phase 4
CONSTRAINT chk_ingestion_status CHECK (ingestion_status IN (
    'RECEIVED','PROCESSING','CLEANSED',
    'TRANSFORMED','CURATED','FAILED','QUARANTINED'
))
```

**Lifecycle:** `RECEIVED → CLEANSED → TRANSFORMED → CURATED`
**On failure at any stage:** `→ FAILED` with `error_message` populated

#### pipeline_run
```sql
run_id           BIGSERIAL PRIMARY KEY,
pipeline_name    VARCHAR(100) NOT NULL,
run_status       VARCHAR(50) NOT NULL,
started_at       TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
ended_at         TIMESTAMP,
records_read     INT DEFAULT 0,
records_written  INT DEFAULT 0,
records_rejected INT DEFAULT 0,
error_message    TEXT,
CONSTRAINT chk_run_status CHECK (run_status IN (
    'RUNNING','SUCCESS','FAILED','PARTIAL'
))
```

---

## FileMeta Dataclass

Parse once at entry point, pass everywhere. Never pass raw filename strings downstream.

```python
@dataclass(frozen=True)
class FileMeta:
    file_name:   str   # BCBS001_837P_PROD_20260312_001.csv
    client_code: str   # BCBS001
    file_type:   str   # 837P
    env:         str   # PROD
    date:        str   # 20260312
    sequence:    str   # 001

    @property
    def stem(self) -> str:
        return self.file_name.rsplit(".", 1)[0]

    @property
    def partition(self) -> str:
        return f"{self.client_code}/{self.file_type}/{self.env}/{self.date}_{self.sequence}"

    @property
    def is_prod(self) -> bool:
        return self.env == "PROD"
```

`parse_filename()` validates regex + real calendar date. Raises `ValueError` with clear message if convention violated.

---

## Airflow DAG — claims_pipeline

### 7-Task Flow
```
check_file_exists
        ↓
run_claims_cleanse          (BashOperator → spark-submit --file-name)
        ↓
validate_cleansed_output    (PythonOperator → checks partitioned prefix)
        ↓
run_claims_transform        (BashOperator → spark-submit --file-name)
        ↓
validate_transformed_output (PythonOperator → checks partitioned prefix)
        ↓
run_claims_curate           (BashOperator → spark-submit --file-name)
        ↓
validate_curated_output     (PythonOperator → checks all 3 curated prefixes)
```

### DAG Config
```
dag_id:      claims_pipeline
schedule:    None  ← event-driven, not time-driven
catchup:     False
retries:     2
retry_delay: 5 minutes
owner:       data-engineering
tags:        healthcare, claims, pipeline
```

### Key Design Decisions
- `schedule=None` — pipeline only runs when a file is dropped
- `get_meta(**context)` — parses FileMeta from `dag_run.conf` at every task
- `check_parquet_files()` — extracted helper, validates file-specific prefix not entire bucket
- `update_file_registry()` — extracted helper, DRY across all validate tasks
- Fail-fast — validate between every stage, stop pipeline on first failure
- `AIRFLOW__API__AUTH_BACKENDS = airflow.api.auth.backend.basic_auth` required for REST API

### Trigger
```bash
# Manual trigger
docker exec airflow-webserver airflow dags trigger claims_pipeline \
  --conf '{"file_name": "BCBS001_837P_PROD_20260312_001.csv"}'

# Via REST API (used by file watcher)
curl -u admin:admin123 \
  -X POST http://localhost:8082/api/v1/dags/claims_pipeline/dagRuns \
  -H "Content-Type: application/json" \
  -d '{"conf": {"file_name": "BCBS001_837P_PROD_20260312_001.csv"}}'
```

---

## PySpark Jobs

### Shared Patterns (All Three Jobs)
```python
# Args
parser.add_argument("--file-name", required=True)
meta = parse_filename(args.file_name)   # FileMeta — single source of truth

# SparkSession — all credentials from env vars
MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT",   "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin123")

# Error handling
try:
    # pipeline logic
    log_to_postgres(status="SUCCESS", ...)
except Exception as e:
    log_to_postgres(status="FAILED", error_msg=str(e))
    raise
finally:
    spark.stop()
```

### claims_cleanse.py
```
Reads:   s3a://healthcare-raw/{partition}/{filename}
Writes:  s3a://healthcare-cleansed/claims/{partition}/   (Parquet)
         s3a://healthcare-quarantine/claims/{partition}/  (Parquet, if any)

Steps:
1. Read CSV from MinIO
2. Validate (claim_id, member_id, provider_npi, billed_amount > 0)
3. Split clean vs quarantine with validation_errors column
4. PHI masking — sha2(member_id, 256) → member_id_hash,
                  sha2(provider_npi, 256) → provider_npi_hash
                  original columns dropped
5. Add lineage columns: source_file, client_code, file_type, env, processed_at
6. Write cleansed parquet
7. Write quarantine parquet (conditional on quarantine_count > 0)
8. Log to pipeline_run
```

### claims_transform.py
```
Reads:   s3a://healthcare-cleansed/claims/{partition}/ (Parquet)
         s3a://healthcare-reference/cpt_codes.csv
         s3a://healthcare-reference/icd10_codes.csv
Writes:  s3a://healthcare-transformed/claims/{partition}/         (Delta)
         s3a://healthcare-transformed/summaries/payer/{partition}/ (Delta)
         s3a://healthcare-transformed/summaries/cpt/{partition}/   (Delta)

Steps:
1. Read cleansed parquet
2. Load CPT + ICD10 reference data
3. Left join CPT (adds cpt_description, cpt_category, typical_duration_mins)
4. Left join ICD10 (adds icd10_description, icd10_category, chronic_flag)
5. Standardize: billed_amount 2dp, place_of_service code→description
6. Derived fields: billed_category (LOW/MEDIUM/HIGH), claim_age_days, chronic_flag bool
7. Write transformed Delta
8. Build payer summary aggregation
9. Build CPT summary aggregation
10. Write summaries as Delta
11. Log to pipeline_run

Uses: configure_spark_with_delta_pip(builder).getOrCreate()
```

### claims_curate.py
```
Reads:   s3a://healthcare-transformed/claims/{partition}/ (Delta)
Writes:  s3a://healthcare-curated/member_summary/{partition}/   (Delta)
         s3a://healthcare-curated/payer_summary/{partition}/    (Delta)
         s3a://healthcare-curated/provider_summary/{partition}/ (Delta)

member_summary:
  member_id_hash, total_claims, total_billed, avg_billed,
  unique_providers, unique_payers, has_chronic_condition,
  most_recent_service_date, curated_at

payer_summary:
  payer_id, total_claims, total_billed, avg_billed,
  unique_members, unique_providers, avg_claim_age_days,
  low_claims, medium_claims, high_claims, curated_at

provider_summary:
  provider_npi_hash, total_claims, total_billed, avg_billed,
  unique_members, unique_payers, unique_cpt_codes, curated_at

Pattern: cache() transformed_df → 3 aggregations → unpersist()
Single scan, three outputs.
```

---

## spark-submit Commands

### Cleanse
```bash
spark-submit \
  --master local[*] \
  --packages org.apache.hadoop:hadoop-aws:3.3.4,\
com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  /opt/airflow/processing/pyspark/claims_cleanse.py \
  --file-name BCBS001_837P_PROD_20260312_001.csv
```

### Transform / Curate (Delta)
```bash
spark-submit \
  --master local[*] \
  --packages io.delta:delta-spark_2.12:3.0.0,\
org.apache.hadoop:hadoop-aws:3.3.4,\
com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=\
org.apache.spark.sql.delta.catalog.DeltaCatalog \
  /opt/airflow/processing/pyspark/claims_transform.py \
  --file-name BCBS001_837P_PROD_20260312_001.csv
```

---

## File Watcher

Watches `data/inbound/`, validates filenames, uploads to MinIO, triggers Airflow DAG.

### Flow
```
File dropped → data/inbound/
      ↓
wait_for_file_stable()   ← guards against partial writes
      ↓
parse_filename()
      ↓ invalid → upload to s3a://healthcare-quarantine/hold/{ts}_{file}
                  delete local file
                  (fallback: data/hold/ if MinIO unreachable)
      ↓ valid →
is_already_processed()   ← check file_registry, skip if CURATED
      ↓
upload to s3a://healthcare-raw/{partition}/{filename}
delete local file        ← MinIO is system of record
      ↓
POST /api/v1/dags/claims_pipeline/dagRuns
  {"conf": {"file_name": "BCBS001_837P_PROD_20260312_001.csv"}}
```

### Startup Scan
When watcher starts, it processes any `.csv` files already sitting in `data/inbound/` automatically. Solves the problem of files arriving while watcher was down.

### Run Modes
```bash
# Foreground
python scripts/file_watcher.py

# Background
nohup python scripts/file_watcher.py > logs/file_watcher.log 2>&1 &
tail -f logs/file_watcher.log
kill $(cat logs/file_watcher.pid)
```

---

## Recovery Tool

`scripts/recover_unprocessed.py` — scans `data/inbound/` for files that arrived while watcher was down. Cross-references `file_registry`, offers per-file confirmation.

```bash
python scripts/recover_unprocessed.py --dry-run      # show only, no changes
python scripts/recover_unprocessed.py                 # per-file y/N/q
python scripts/recover_unprocessed.py --file-name X  # specific file
python scripts/recover_unprocessed.py --force        # re-trigger even CURATED
```

---

## Trino Query Engine

Trino 435 as Docker service. No data storage — SQL layer on top of existing storage.

### Catalogs
```
delta      → reads Delta Lake tables from MinIO (native Delta connector)
postgresql → queries pipeline_db audit tables
system     → built-in Trino system catalog
```

### Register Tables
```sql
CREATE SCHEMA IF NOT EXISTS delta.healthcare
WITH (location = 's3a://healthcare-curated/');

CALL delta.system.register_table(
    schema_name    => 'healthcare',
    table_name     => 'member_summary',
    table_location => 's3a://healthcare-curated/member_summary/BCBS001/837P/PROD/20260312_001/'
);
-- Same for payer_summary, provider_summary
```

### Cross-Source Query Example
```sql
-- MinIO Delta + PostgreSQL in a single SQL statement
SELECT
    f.file_name, f.client_code, f.env, f.ingestion_status,
    p.records_read, p.records_written, p.run_status,
    date_diff('second', p.started_at, p.ended_at) AS duration_secs
FROM postgresql.public.file_registry f
JOIN postgresql.public.pipeline_run p
    ON p.pipeline_name = 'claims_curate_job'
ORDER BY f.received_at DESC;
```

Note: Use `date_diff('second', ...)` not `EXTRACT(EPOCH ...)` — Trino doesn't support EPOCH.

---

## Streamlit Dashboard

```bash
cd frontend && streamlit run app.py
# Opens at http://localhost:8501
```

### Pages
**Home (`app.py`)** — pipeline flow diagram, tech stack cards

**Pipeline Monitor (`01_pipeline_monitor.py`)**
- KPIs: total files, curated, failed, unique clients, avg duration
- File registry with color-coded ingestion status
- Pipeline run history with durations
- File → pipeline lineage (cross-source Trino query)
- 30-second cache TTL, refresh button

**Claims Analytics (`02_claims_analytics.py`)**
- Member KPIs: total members, claims, billed, chronic count
- Payer bar chart + stacked LOW/MEDIUM/HIGH breakdown
- Provider top-10 horizontal bar + scatter bubble chart
- Member chronic donut + billed distribution histogram
- All charts: Plotly dark theme, 60-second cache TTL

---

## Custom Airflow Dockerfile

```dockerfile
FROM apache/airflow:2.9.1
USER root
RUN apt-get update && apt-get install -y openjdk-17-jdk && apt-get clean
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
USER airflow
RUN pip install --no-cache-dir \
    pyspark==3.5.1 \
    delta-spark==3.0.0 \
    minio==7.2.5 \
    psycopg2-binary==2.9.9 \
    apache-airflow-providers-apache-spark==4.7.1
```

**Why pin pyspark==3.5.1:** Airflow 2.9.1 default pip resolution installs PySpark 4.x which breaks Delta Lake 3.0.0 and Spark 3.5.1 cluster compatibility.

---

## Issues Fixed (Full Reference)

| Issue | Fix |
|---|---|
| Airflow PySpark 4.x vs 3.5.1 mismatch | Pin pyspark==3.5.1 in Dockerfile |
| MinIO endpoint localhost vs minio | Use container name: http://minio:9000 |
| Delta ClassNotFoundException | Add io.delta:delta-spark_2.12:3.0.0 to --packages |
| Delta version conflict | Use delta-spark==3.0.0 + delta-spark_2.12:3.0.0 JAR |
| NumberFormatException "60s" | Use numeric ms: "600000" not "60s" |
| hadoop-aws version mismatch | Use 3.3.4 consistently across all spark-submit |
| YAML indentation | Always 2 spaces, never tabs |
| count() called multiple times | Cache: raw_count = raw_df.count() |
| Airflow REST API 403 | Add AIRFLOW__API__AUTH_BACKENDS=basic_auth |
| Trino EXTRACT(EPOCH) error | Use date_diff('second', ...) instead |
| Trino memory crash | JVM 1.5GB heap, query.max-memory-per-node=1GB |
| Trino register_table disabled | Add delta.register-table-procedure.enabled=true |
| Docker mounts creating directories | Create config files before docker-compose up |
| zsh ! history expansion | Use single quotes: echo '!data/raw/.gitkeep' |

---

## Key Commands

```bash
# Stack
docker-compose up -d --build
docker-compose down
docker ps
docker logs airflow-scheduler --tail 50 -f

# Airflow
docker exec airflow-webserver airflow dags trigger claims_pipeline \
  --conf '{"file_name": "BCBS001_837P_PROD_20260312_001.csv"}'
docker exec airflow-webserver airflow dags list-runs -d claims_pipeline

# Spark (manual run)
docker exec --user airflow -it airflow-scheduler bash
# then spark-submit ...

# PostgreSQL
docker exec -it postgres psql -U pgadmin -d pipeline_db
docker exec -i postgres psql -U pgadmin -d pipeline_db < migrate_file_registry.sql

# Trino
docker exec -it trino trino

# Streamlit
cd frontend && streamlit run app.py

# File watcher
python scripts/file_watcher.py
python scripts/recover_unprocessed.py --dry-run

# Git
git log --oneline
git add . && git commit -m "message"

# Verify env vars in container
docker exec airflow-scheduler env | grep POSTGRES
docker exec airflow-scheduler env | grep MINIO
```

---

## Architecture Design Decisions

| Decision | Choice | Reason |
|---|---|---|
| Cleansed format | Parquet | Ephemeral staging, no version history needed |
| Transform/Curate | Delta Lake | ACID, time travel, schema enforcement |
| Path structure | Partitioned CLIENT/FILE_TYPE/ENV/DATE_SEQ | Spark predicate pushdown |
| Credentials | os.getenv() + .env | Cloud-portable, never committed to Git |
| FileMeta | frozen dataclass | Type safety, immutability, computed properties |
| Scheduler | None (schedule=None) | Event-driven, not time-driven |
| Invalid files | MinIO quarantine/hold/ | All arrivals preserved, MinIO as system of record |
| Local inbound | Delete after upload | No local duplication, MinIO is system of record |
| PHI masking | SHA-256 hash, keep as new column | One-way, but same input = same hash for cross-file joins |
| Trino memory | 1GB query / 1.5GB JVM | Fits within Docker 8GB on Mac Mini |
| data/raw/ local | Retired | MinIO healthcare-raw/ replaces it entirely |

---

## What's Working ✅

1. Full Docker stack — MinIO, PostgreSQL, Spark, Airflow, Trino, Streamlit
2. `claims_cleanse.py` — CSV ingestion, validation, PHI masking, parquet output
3. `claims_transform.py` — CPT/ICD10 enrichment, derived fields, Delta output
4. `claims_curate.py` — member/payer/provider reporting summaries as Delta
5. 7-task Airflow DAG — fully dynamic, event-driven, fail-fast
6. `path_utils.py` — FileMeta dataclass, partitioned paths, all zones
7. `file_watcher.py` — detect → validate → upload MinIO → trigger DAG
8. `recover_unprocessed.py` — CLI recovery with per-file confirmation
9. Trino — Delta + PostgreSQL catalogs, cross-source SQL working
10. Streamlit — pipeline monitor + claims analytics, dark theme
11. `file_registry` — full lifecycle audit per file with 4 new columns
12. `.env` + env vars — no hardcoded credentials anywhere

---

## Roadmap

### Phase 8 — Enterprise Ingestion Framework (Next)

#### 8a — Package Structure + Plugin Architecture
```
processing/
  common/
    stages/
      __init__.py
      ingest.py           ← read_file() router
      header_trailer.py   ← envelope validation
      validate.py
      mask.py
      enrich.py
      derive.py
      write.py
    path_utils.py
    client_config.py      ← loads config/clients/{CLIENT}/{FILE_TYPE}.yaml

  clients/
    default/              ← base pipeline
      cleanse.py
      transform.py
      curate.py
    BCBS001/
      config.yaml
      transform.py        ← optional override
    AETNA02/              ← second client, proves multi-tenancy
      config.yaml

config/
  clients/
    BCBS001/
      837P.yaml
    AETNA02/
      837P.yaml
```

#### 8b — PySpark SQL Stages
Replace DataFrame API chains with SQL stages — readable, overridable per client:
```python
spark.sql("""
    SELECT *,
        CASE WHEN billed_amount < 200 THEN 'LOW'
             WHEN billed_amount < 500 THEN 'MEDIUM'
             ELSE 'HIGH'
        END AS billed_category
    FROM cleansed_claims
""")
```
Client drops a custom `.sql` file to override a stage — no Python changes needed.

#### 8c — Delimiter Support + Auto-Detection
```yaml
format:
  type: delimited
  delimiter: auto   # auto | "," | "|" | "\t" | custom
```

#### 8d — Header/Trailer Envelope Validation
Four file header cases:
```
Case A: column names in first row (current)
Case B: column names + @HEADER/@TRAILER
Case C: @HEADER/@TRAILER, no column names → schema from config
Case D: raw data only → schema from config
```

Envelope format:
```
@HEADER|BCBS001|837P|PROD|20260312|001|DELIMITER=,|ROW_COUNT=8
... data rows ...
@TRAILER|BCBS001|837P|PROD|20260312|001|ROW_COUNT=8|CHECKSUM=abc123
```

Three-way count: `header_count == actual_rows == trailer_count` → proceed, else quarantine.

#### 8e — Schema from Config (Cases C and D)
```yaml
schema:
  source: config
  fields:
    - name: claim_id
      type: string
field_mappings:
  charged_amt: billed_amount
  subscriber_id: member_id
```

#### 8f — Fixed Width Support
```yaml
format:
  type: fixed_width
  record_length: 100
  fields:
    - name: claim_id
      start: 0
      end: 10
      type: string
      trim: true
    - name: billed_amount
      start: 25
      end: 35
      type: decimal
      scale: 2
```

#### 8g — Second Client (AETNA02)
Different field names, pipe-delimited, optional logic override. Proves multi-tenancy end-to-end.

---

### Phase 9 — Operational Improvements

#### 9a — Live Pipeline Progress (Streamlit Page 3)
Poll Airflow REST API every 5 seconds, show task states in real time.
Note: Airflow UI already does this — only build if non-engineers need it.

#### 9b — Pipeline Restart from Failed Stage
```bash
python scripts/restart_pipeline.py \
  --file-name BCBS001_837P_PROD_20260312_001.csv \
  --from-task validate_cleansed_output
```
Uses Airflow REST API to clear and re-run from specific task.

---

### Phase 10 — Scala Spark Job
Rewrite `claims_cleanse.py` in Scala. Same FileMeta, same paths, same PostgreSQL logging.
```
processing/scala/
  build.sbt
  src/main/scala/com/healthcare/pipeline/
    ClaimsCleanse.scala
    FileMeta.scala
    PathUtils.scala
```
Shows JVM-side Spark competency. Relevant for Databricks-heavy, fintech, large health system roles.

Note: Separate from PySpark SQL (Phase 8b).
- PySpark SQL = Python + SQL-first → enables client overrides
- Scala Spark = JVM language → shows JVM competency

---

### Phase 11 — README as Architecture Document
Not a setup guide — a proper architecture doc:
- Overview, Architecture Diagram (Mermaid)
- Pipeline flow, Filename convention, Zone map
- Client config system, Design decisions & tradeoffs
- Running locally, Tech stack & version matrix

---

### Phase 12 — Kafka Streaming Ingestion (Optional)
- Kafka producer simulating real-time claim events
- Spark Structured Streaming consumer
- Land micro-batches into `healthcare-raw/` with same filename convention

---

### Future Phases
```
Phase 13: JSON/XML → HL7 FHIR clinical data
Phase 14: HL7 v2 message parsing (segment-based)
Phase 15: Unstructured → clinical notes, PDFs via OCR
Phase 16: FastAPI on top of Trino (public API layer)
Phase 17: dbt models on top of Trino for analytics
```

---

## Interview Talking Points

**On the overall architecture:**
> "The pipeline is event-driven — a file watcher monitors an inbound directory, validates
> the filename convention, uploads to MinIO, and triggers Airflow via REST API. The DAG
> runs a 7-task fail-fast pipeline with validation between every stage. All data is Delta
> Lake in MinIO, queryable via Trino. Credentials are env vars — cloud-portable by design."

**On Delta vs Parquet:**
> "Cleansed is Parquet because it's ephemeral staging — if something goes wrong we re-cleanse
> from raw, so version history adds no value. Delta starts at transform where data has business
> value worth protecting: ACID transactions, time travel, schema enforcement."

**On PHI masking:**
> "SHA-256 hash on member_id and provider_npi, keeping hashed columns. One-way — can't reverse.
> But same input always produces same hash, so you can still join across files without exposing
> real PHI. Production would add a secret salt in KMS to prevent rainbow table attacks."

**On the filename convention:**
> "Self-describing — client, file type, environment, date, sequence. ENV in filename because
> clients send via SFTP and can't control folder structure. Sequence handles same-day re-sends
> without overwriting. Idempotency enforced by convention before code."

**On Phase 8 client config (future):**
> "The pipeline is a plugin system. Common stages are a package. Each client has a YAML defining
> field mappings, validation rules, delimiter, schema source, and envelope format. Client logic
> that can't be expressed in config lives in an optional strategy module. Onboarding a new client
> is dropping a config folder — no core code changes."

**On partitioned paths:**
> "Partitioned by CLIENT/FILE_TYPE/ENV so Spark can prune at directory level. Reading all BCBS001
> professional claims across dates scans only claims/BCBS001/837P/PROD/*/ — not the entire bucket.
> DATE and SEQUENCE kept together as the leaf folder — they're typically queried as a unit."