# Healthcare Data Platform — Session Summary & Roadmap

## What We Built Today

### 1. Dynamic Filename Convention
Replaced hardcoded `BCBS001_837P_20260312.csv` with a structured convention:

```
{CLIENT_CODE}_{FILE_TYPE}_{ENV}_{DATE}_{SEQUENCE}.csv
BCBS001_837P_PROD_20260312_001.csv
```

| Part | Rule | Example |
|---|---|---|
| CLIENT_CODE | Uppercase alphanumeric, 3-10 chars | BCBS001 |
| FILE_TYPE | 837P, 837I, 835, MEMBER, PHARMACY, CROSSWALK | 837P |
| ENV | PROD, TEST, SYS1, SYS2, UAT1, UAT2 | PROD |
| DATE | YYYYMMDD, validated as real calendar date | 20260312 |
| SEQUENCE | 3-digit zero-padded 001-999 | 001 |

---

### 2. FileMeta Dataclass
Single source of truth — parse once at entry point, pass everywhere:

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
    def partition(self) -> str:
        return f"{self.client_code}/{self.file_type}/{self.env}/{self.date}_{self.sequence}"
```

`parse_filename()` validates regex + real calendar date. Raises `ValueError` with clear message on failure.

---

### 3. Partitioned MinIO Paths
Replaced flat paths with partitioned structure for Spark predicate pushdown:

```
# Before
s3a://healthcare-cleansed/claims/BCBS001_837P_20260312/

# After
s3a://healthcare-cleansed/claims/BCBS001/837P/PROD/20260312_001/
```

All path functions updated in `path_utils.py` to accept `FileMeta` instead of raw string.
Added `scratch_path(label, username)` for temp workspaces with 7-day TTL convention.

---

### 4. Env Vars Across All Services
Replaced all hardcoded credentials with `os.getenv()` + fallback defaults:

```python
POSTGRES_HOST     = os.getenv("POSTGRES_HOST",     "postgres")
MINIO_ENDPOINT    = os.getenv("MINIO_ENDPOINT",    "http://minio:9000")
MINIO_ACCESS_KEY  = os.getenv("MINIO_ACCESS_KEY",  "minioadmin")
```

`.env` file at project root, referenced by all Docker services via `env_file: - .env`.
`.env` in `.gitignore`, `.env.example` committed with placeholder values.

---

### 5. PySpark Jobs Refactored (All Three)
`claims_cleanse.py`, `claims_transform.py`, `claims_curate.py` all updated to:
- `argparse --file-name` instead of hardcoded filename
- `parse_filename()` → `FileMeta` as single source of truth for all paths
- `sys.path.insert(0, "/opt/airflow/processing")` for `common/` importability in spark-submit
- `elapsed` time computed and logged
- `run_time` passed into `log_to_postgres()` as param, not module-level global
- `client_code`, `file_type`, `env` added as lineage columns in cleansed output

`claims_curate.py` (new) produces three reporting-ready Delta summaries:
- `member_summary` — per member cost profile, chronic flag, unique providers/payers
- `payer_summary` — per payer billing breakdown with LOW/MEDIUM/HIGH claim counts
- `provider_summary` — per provider billing, unique members, CPT diversity
- `cache()` / `unpersist()` pattern — single scan for all three aggregations

---

### 6. Airflow DAG — 7 Tasks
Updated from 5 to 7 tasks, fully dynamic:

```
check_file_exists
    ↓
run_claims_cleanse          --file-name {{ dag_run.conf['file_name'] }}
    ↓
validate_cleansed_output    checks claims/BCBS001/837P/PROD/20260312_001/
    ↓
run_claims_transform        --file-name {{ dag_run.conf['file_name'] }}
    ↓
validate_transformed_output checks claims/BCBS001/837P/PROD/20260312_001/
    ↓
run_claims_curate           --file-name {{ dag_run.conf['file_name'] }}
    ↓
validate_curated_output     checks all 3 curated summary prefixes
```

Key improvements:
- `get_meta(**context)` — parses FileMeta from `dag_run.conf` at every task
- `check_parquet_files()` — extracted helper, reused across all validate tasks
- `update_file_registry()` — extracted helper, DRY across all validate tasks
- Validates file-specific partitioned prefix, not entire bucket
- `schedule=None` — event-driven, not time-driven
- `AIRFLOW__API__AUTH_BACKENDS = airflow.api.auth.backend.basic_auth`

Trigger:
```bash
docker exec airflow-webserver airflow dags trigger claims_pipeline \
  --conf '{"file_name": "BCBS001_837P_PROD_20260312_001.csv"}'
```

---

### 7. File Watcher (`scripts/file_watcher.py`)
Watches `data/inbound/`, validates, uploads to MinIO, triggers DAG automatically.

Flow:
```
File dropped into data/inbound/
    ↓
Validate filename convention (regex + date check)
    ↓ invalid → upload to s3a://healthcare-quarantine/hold/{timestamp}_{file}
    ↓           delete from local inbound
    ↓ valid →
Check file_registry — already CURATED? → skip with warning
    ↓
Upload to s3a://healthcare-raw/{CLIENT}/{FILE_TYPE}/{ENV}/{DATE_SEQ}/{file}
    ↓ delete from local inbound (MinIO is system of record)
    ↓
POST /api/v1/dags/claims_pipeline/dagRuns
    {"conf": {"file_name": "BCBS001_837P_PROD_20260312_001.csv"}}
```

Key features:
- `wait_for_file_stable()` — guards against partial file writes
- `process_file()` extracted — shared by `on_created` event AND startup scan
- Startup scan — processes files already in inbound when watcher starts
- Invalid files → `s3a://healthcare-quarantine/hold/` then deleted locally
- Fallback to `data/hold/` only if MinIO unreachable
- PID file at `logs/file_watcher.pid` for background process management

Run modes:
```bash
# Foreground
python scripts/file_watcher.py

# Background
nohup python scripts/file_watcher.py > logs/file_watcher.log 2>&1 &
kill $(cat logs/file_watcher.pid)
```

---

### 8. Recovery Tool (`scripts/recover_unprocessed.py`)
Scans `data/inbound/` for files that arrived while watcher was down.

```bash
python scripts/recover_unprocessed.py --dry-run       # show what would happen
python scripts/recover_unprocessed.py                  # per-file y/N/q confirmation
python scripts/recover_unprocessed.py --file-name X   # specific file only
python scripts/recover_unprocessed.py --force         # re-trigger even CURATED
```

- Color-coded summary table: TRIGGER (green) / SKIP (yellow) / INVALID (red)
- Invalid files → MinIO quarantine automatically, no confirmation needed
- Valid files → upload to MinIO raw, delete local, trigger DAG
- Already-in-MinIO check — skips re-upload, still deletes local duplicate

---

### 9. PostgreSQL Migration
Four new columns on `file_registry`:

```sql
ALTER TABLE file_registry
    ADD COLUMN IF NOT EXISTS client_code VARCHAR(10),
    ADD COLUMN IF NOT EXISTS file_type   VARCHAR(20),
    ADD COLUMN IF NOT EXISTS env         VARCHAR(10),
    ADD COLUMN IF NOT EXISTS sequence    VARCHAR(3);
```

Full lifecycle: `RECEIVED → CLEANSED → TRANSFORMED → CURATED → FAILED`

---

### 10. Trino Query Engine
Added Trino 435 as Docker service on port 8083.

Catalogs:
- `delta` — reads Delta Lake tables from MinIO via native Delta connector
- `postgresql` — queries `pipeline_db` audit tables directly
- `system` — built-in Trino system catalog

Config files:
```
infra/trino/
  config.properties     ← coordinator config, memory limits
  node.properties       ← node identity
  jvm.config            ← 1.5GB heap
  catalog/
    delta.properties    ← Delta connector → MinIO S3A
    postgresql.properties ← PostgreSQL connector → pipeline_db
```

Tables registered:
```sql
CALL delta.system.register_table(
    schema_name    => 'healthcare',
    table_name     => 'member_summary',
    table_location => 's3a://healthcare-curated/member_summary/BCBS001/837P/PROD/20260312_001/'
);
-- Same for payer_summary, provider_summary
```

Cross-source query example:
```sql
SELECT
    f.file_name, f.client_code, f.env, f.ingestion_status,
    p.records_read, p.records_written, p.run_status,
    date_diff('second', p.started_at, p.ended_at) AS duration_secs
FROM postgresql.public.file_registry f
JOIN postgresql.public.pipeline_run p
    ON p.pipeline_name = 'claims_curate_job'
ORDER BY f.received_at DESC;
```

Note: use `date_diff('second', ...)` not `EXTRACT(EPOCH ...)` — Trino doesn't support EPOCH.

---

### 11. Streamlit Dashboard
Three-page app at `http://localhost:8501`:

```
frontend/
  app.py                      ← landing page, pipeline flow diagram, tech stack
  pages/
    01_pipeline_monitor.py    ← operational view
    02_claims_analytics.py    ← analytical view
```

`app.py` — hero section with full pipeline flow, tech stack cards, navigation.

`01_pipeline_monitor.py`:
- KPI metrics: total files, curated, failed, unique clients, avg duration
- File registry table with color-coded ingestion status
- Pipeline run history with duration
- File → pipeline lineage (cross-source Trino query)
- 30-second cache TTL, refresh button

`02_claims_analytics.py`:
- Member KPIs: total members, claims, billed, avg billed, chronic count
- Payer bar chart (total billed) + stacked bar (LOW/MEDIUM/HIGH)
- Provider horizontal bar (top 10) + scatter (claims vs billed, bubble = members)
- Member donut (chronic vs non-chronic) + histogram (billed distribution)
- All charts: Plotly dark theme, 60-second cache TTL

Install:
```bash
pip install streamlit trino plotly
cd frontend && streamlit run app.py
```

---

### 12. Data Architecture Decisions Made Today

| Decision | Choice | Reason |
|---|---|---|
| Cleansed format | Parquet | Ephemeral staging, no version history needed |
| Transform/Curate format | Delta Lake | ACID transactions, time travel, schema enforcement |
| Path structure | Partitioned by CLIENT/FILE_TYPE/ENV | Spark predicate pushdown, cross-date analytics |
| Credentials | os.getenv() + .env | Cloud-portable, never committed to Git |
| FileMeta | frozen dataclass | Type safety, immutability, computed properties |
| Scheduler | None | Event-driven architecture, not time-driven |
| Invalid files | MinIO quarantine/hold/ | Preserve all arrivals, MinIO as system of record |
| local inbound | Delete after upload | MinIO is system of record, no local duplication |
| Trino memory | 1GB query / 1.5GB JVM | Fits within Docker 8GB limit on Mac Mini M4 |

---

## Git History (Today's Commits)

```
fd4b61c  chore: clean up duplicate gitignore entries, ignore data files
aa47d86  fix: set schedule=None, pipeline is event-driven not time-driven
623ab32  feat: file watcher with MinIO quarantine, startup scan, local cleanup
273ebe3  infra: add Trino service to docker-compose
d89e3df  feat: add Trino with Delta and PostgreSQL catalogs
69f25d2  feat: file watcher with auto-trigger, FileMeta convention, partitioned MinIO paths
49969eb  feat: add curate + validate tasks to DAG, dynamic filename from dag_run.conf
63da8dc  feat: add claims_curate.py — member, payer, provider summaries as Delta
912c42b  refactor: dynamic filename via argparse, env vars in claims_transform
29b6517  refactor: dynamic filename via argparse, env vars in claims_cleanse
a06a6b8  feat: add curated path helpers to path_utils
```

---

## Complete Roadmap — What's Next

### Phase 8 — Enterprise Ingestion Framework (HIGHEST PRIORITY)

#### 8a — Package Structure + Plugin Architecture
Restructure into a plugin system:

```
processing/
  common/
    stages/
      __init__.py
      ingest.py           ← read_file() router (delimited/fixed/json/xml)
      header_trailer.py   ← envelope validation (@HEADER/@TRAILER)
      validate.py         ← schema + business rule validation
      mask.py             ← PHI masking
      enrich.py           ← CPT/ICD10 reference joins
      derive.py           ← derived fields, categories, flags
      write.py            ← write_parquet(), write_delta()
    path_utils.py         ← already exists
    client_config.py      ← NEW: loads config/clients/{CLIENT}/{FILE_TYPE}.yaml

  clients/
    default/              ← base pipeline all clients inherit
      cleanse.py
      transform.py
      curate.py
    BCBS001/
      config.yaml         ← field mappings, rules, thresholds
      transform.py        ← optional stage override
    AETNA02/              ← second client, proves multi-tenancy
      config.yaml
      cleanse.py          ← custom override example

config/
  clients/
    BCBS001/
      837P.yaml
      MEMBER.yaml
    AETNA02/
      837P.yaml
```

#### 8b — PySpark SQL Stages
Replace DataFrame API chains with SQL in `.sql` files:

```python
# Instead of DataFrame chaining
df.withColumn("billed_category",
    when(col("billed_amount") < 200, lit("LOW"))...)

# SQL stage — readable, overridable per client
spark.sql("""
    SELECT *,
        CASE
            WHEN billed_amount < 200 THEN 'LOW'
            WHEN billed_amount < 500 THEN 'MEDIUM'
            ELSE 'HIGH'
        END AS billed_category
    FROM cleansed_claims
""")
```

Client-specific SQL override — AETNA02 drops a custom `transform.sql`, no Python changes needed.

#### 8c — Delimiter Support + Auto-Detection
Config YAML:
```yaml
format:
  type: delimited
  delimiter: auto     # auto | "," | "|" | "\t" | custom
```
Auto-detection: read first data line, count occurrences of candidate chars.
Predefined: client specifies in YAML, overrides auto-detection.

#### 8d — Header/Trailer Envelope Validation
Four file header cases:

```
Case A: column names in first row only (current behavior)
Case B: column names + @HEADER/@TRAILER envelope
Case C: @HEADER/@TRAILER but NO column names (schema from config YAML)
Case D: raw data only — no names, no envelope (schema from config YAML)
```

Envelope format:
```
@HEADER|BCBS001|837P|PROD|20260312|001|DELIMITER=,|ROW_COUNT=8
CLM001,MBR001,...
CLM002,MBR002,...
@TRAILER|BCBS001|837P|PROD|20260312|001|ROW_COUNT=8|CHECKSUM=abc123
```

Three-way count validation:
```
header_count == actual_row_count == trailer_count → proceed
any mismatch → quarantine entire file with detailed error
```

#### 8e — Schema from Config (Cases C and D)
```yaml
schema:
  source: config        # file | config | both
  fields:
    - name: claim_id
      type: string
    - name: billed_amount
      type: decimal

field_mappings:         # rename incoming → standard column names
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
      scale: 2          # last 2 digits are decimals
    - name: service_date
      start: 35
      end: 43
      type: date
      format: YYYYMMDD
```

Implemented via `spark.read.text()` + `substr()` per field definition.

#### 8g — Second Client (AETNA02)
- Different field names: `charged_amt` → `billed_amount`
- Pipe-delimited instead of comma
- Custom transformation logic override
- Proves the plugin system works end-to-end with real data

---

### Phase 9 — Operational Improvements

#### 9a — Live Pipeline Progress from UI
- Streamlit page 3: `03_pipeline_progress.py`
- Polls Airflow REST API every 5 seconds
- Shows current DAG run task states in real time
- Color-coded: queued / running / success / failed per task
- Note: Airflow UI already does this — only build if non-engineers need it

#### 9b — Pipeline Restart from Failed Stage
Backend script `scripts/restart_pipeline.py`:
```bash
# Clear failed task and re-run from that point
python scripts/restart_pipeline.py \
  --file-name BCBS001_837P_PROD_20260312_001.csv \
  --from-task validate_cleansed_output

# Force re-run entire pipeline
python scripts/restart_pipeline.py \
  --file-name BCBS001_837P_PROD_20260312_001.csv \
  --force-all
```
Uses Airflow REST API: `PATCH /api/v1/dags/{dag_id}/dagRuns/{run_id}/taskInstances/{task_id}`
Optional: add "Restart" button to Streamlit pipeline monitor page.

---

### Phase 10 — Scala Spark Job
Rewrite `claims_cleanse.py` in Scala:

```
processing/scala/
  build.sbt
  src/main/scala/
    com/healthcare/pipeline/
      ClaimsCleanse.scala
      FileMeta.scala
      PathUtils.scala
```

- Same FileMeta convention, same MinIO paths, same PostgreSQL logging
- Build as SBT project
- Shows JVM-side Spark competency
- Relevant for roles at companies running Scala-first Spark (Databricks heavy, fintech, large health systems)

Note: This is SEPARATE from PySpark SQL (Phase 8b).
- PySpark SQL = still Python, SQL-first transformations → enables client overrides
- Scala Spark = rewrite in JVM language → shows JVM competency

---

### Phase 11 — README as Architecture Document
Not a setup guide — an architecture document:

```
## Overview
## Architecture Diagram (Mermaid)
## Pipeline Flow
## Filename Convention
## MinIO Zone Map
## Client Config System
## Design Decisions & Tradeoffs
## Running Locally
## Tech Stack & Version Matrix
```

This is what gets looked at in interviews before the code.

---

### Phase 12 — Kafka Streaming Ingestion (Optional)
- Kafka producer simulating real-time claim events
- Spark Structured Streaming consumer
- Land micro-batches into `healthcare-raw/` with same filename convention
- File watcher picks up and triggers pipeline as normal
- Add Kafka + Zookeeper to docker-compose

---

### Future Phases
```
Phase 13: JSON/XML support → HL7 FHIR clinical data
Phase 14: HL7 v2 message parsing (segment-based, very common in healthcare)
Phase 15: Unstructured data → clinical notes, PDFs via OCR
Phase 16: FastAPI on top of Trino (if public API exposure needed)
Phase 17: dbt models on top of Trino for analytics layer
```

---

## Key Interview Talking Points

### What to say about the architecture:
> "The pipeline is event-driven — a file watcher monitors an inbound directory,
> validates the filename convention, uploads to MinIO, and triggers Airflow via
> REST API. The DAG runs a 7-task fail-fast pipeline: cleanse → transform → curate,
> with validation between every stage. All data is stored as Delta Lake in MinIO,
> queryable via Trino. Credentials are all env vars — the whole stack is
> cloud-portable by design."

### What to say about Phase 8 (client config):
> "The pipeline is a plugin system. Common stages are a package — read, validate,
> mask, enrich, derive, write. Each client has a YAML config defining field mappings,
> validation rules, delimiter, schema source, and envelope format. Client-specific
> logic that can't be expressed in config lives in an optional strategy module.
> The default pipeline runs if no override exists. Onboarding a new client is
> dropping a config folder — no core code changes."

### What to say about Delta vs Parquet decision:
> "Cleansed is Parquet because it's ephemeral staging — if something goes wrong
> we re-cleanse from raw, so version history adds no value. Delta starts at
> transform where data has business value worth protecting. Delta gives us ACID
> transactions, time travel, and schema enforcement. The format choice matches
> the value and lifecycle of the data at each zone."

### What to say about PHI masking:
> "We SHA-256 hash member_id and provider_npi, keeping the hashed columns.
> SHA-256 is one-way — you can't reverse it. But the same input always produces
> the same hash, so you can still join across files from the same client without
> ever exposing real PHI. In production you'd add a secret salt stored in KMS
> to prevent rainbow table attacks."

### What to say about the filename convention:
> "The filename is self-describing — client code, file type, environment, date,
> and sequence. Environment in the filename because clients send via SFTP and
> can't control folder structure. Sequence handles same-day re-sends without
> overwriting. A re-send is _002 not a duplicate of _001 — idempotency is
> enforced by convention before it's enforced by code."

---

## Quick Reference — Key Commands

```bash
# Start stack
docker-compose up -d --build

# Trigger pipeline
docker exec airflow-webserver airflow dags trigger claims_pipeline \
  --conf '{"file_name": "BCBS001_837P_PROD_20260312_001.csv"}'

# Start file watcher
python scripts/file_watcher.py

# Recover unprocessed files
python scripts/recover_unprocessed.py --dry-run
python scripts/recover_unprocessed.py

# Trino CLI
docker exec -it trino trino

# Streamlit
cd frontend && streamlit run app.py

# Check pipeline runs
docker exec postgres psql -U pgadmin -d pipeline_db -c \
  "SELECT pipeline_name, run_status, records_read, records_written,
          EXTRACT(EPOCH FROM (ended_at - started_at))::int AS secs
   FROM pipeline_run ORDER BY started_at DESC LIMIT 10;"
```

## Service Ports
```
MinIO Console:    localhost:9001   minioadmin / minioadmin123
MinIO API:        localhost:9000
PostgreSQL:       localhost:5432   pgadmin / pgpassword123
Airflow UI:       localhost:8082   admin / admin123
Spark UI:         localhost:8080
Trino:            localhost:8083
Streamlit:        localhost:8501
```