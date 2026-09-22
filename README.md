# DataDissect

A healthcare claims data platform built end to end on open-source tooling — to work
through every layer of a modern lakehouse without a managed platform abstracting it away.

> **This is a personal learning project, not a production system.** It runs locally under
> Docker on a single machine. All data is synthetic. The goal was to build each layer
> deliberately and be able to explain why every design decision was made — the reasoning is
> documented as ADRs in [`docs/architecture/decisions.md`](docs/architecture/decisions.md).

---

> **Azure port:** [github.com/bhumadhi/datadissect-azure](https://github.com/bhumadhi/datadissect-azure)
> — this architecture rebuilt on Azure Databricks with Terraform: ADLS Gen2,
> managed identity, Unity Catalog, and OIDC CI/CD.

## What it does

Ingests healthcare claims files (X12 837P/837I professional and institutional), moves them
through a medallion architecture, and exposes curated Delta tables to SQL and dashboards.

```
  file drop                                                         
      │                                                             
      ▼                                                             
 file_watcher ──▶ validate filename ──▶ upload to object store ──▶ trigger DAG
      │                    │                                        │
      │                    └── invalid ──▶ quarantine               │
      │                                                             ▼
      │                                              ┌──────────────────────────┐
      │                                              │  Airflow  (7 tasks)      │
      │                                              │                          │
      │                                              │  cleanse   ──▶ validate  │
      │                                              │  transform ──▶ validate  │
      │                                              │  curate    ──▶ validate  │
      │                                              └──────────────────────────┘
      │                                                             │
      ▼                                                             ▼
   raw ──────▶ cleansed ──────▶ transformed ──────▶ curated ──▶ Trino ──▶ Streamlit
 (immutable)   (Parquet)          (Delta)           (Delta)
                PHI masked      enriched via      member / payer /
                deduped         reference joins   provider summaries
```

Every stage writes its outcome to a PostgreSQL audit trail (`file_registry`, `pipeline_run`)
so each file's lifecycle is traceable: `RECEIVED → CLEANSED → TRANSFORMED → CURATED`,
or `FAILED` / `QUARANTINED` with the error recorded.

---

## Design decisions

The interesting part of this project is the reasoning, not the code. Full ADRs are in
[`docs/architecture/decisions.md`](docs/architecture/decisions.md); the short version:

| Decision | Why |
|---|---|
| **Parquet at cleansed, Delta from transform on** | Cleansed is re-derivable from raw, so a transaction log buys nothing. Delta starts where data has business value worth protecting — ACID, time travel, schema enforcement. |
| **SHA-256 PHI masking, originals dropped** | One-way but deterministic, so a member's claims still join across files after de-identification. Encryption needs key management; tokenization creates a vault that is itself a PHI liability. Production would add a salt in KMS. |
| **Self-describing filenames** | `{CLIENT}_{TYPE}_{ENV}_{DATE}_{SEQ}.csv` — clients deliver over SFTP and can't control folder structure, so the file carries its own metadata. The sequence number makes same-day re-sends idempotent by convention, before any code runs. |
| **Event-driven DAG, `schedule=None`** | Claims files don't arrive on a timetable. A cron schedule either adds latency or burns compute on empty runs. |
| **Partitioned zone layout** | `{CLIENT}/{TYPE}/{ENV}/{DATE}_{SEQ}/` lets Spark prune at the directory level instead of scanning the bucket. |
| **Object store is the system of record** | The local inbound directory is a landing pad; the file is deleted after upload. Two copies means two sources of truth, and they will diverge. |

---

## Stack

| Layer | Tool | Why |
|---|---|---|
| Storage | MinIO | S3-compatible, stands in for cloud object storage |
| Processing | Apache Spark 3.5.1 / PySpark | Distributed processing, local mode |
| Table format | Delta Lake 3.0.0 | ACID, time travel, schema enforcement |
| Orchestration | Apache Airflow 2.9.1 | Task dependencies, retries, observability |
| Metadata | PostgreSQL 16 | Pipeline audit + file registry |
| Query | Trino 435 | Cross-source SQL over Delta and PostgreSQL |
| Frontend | Streamlit | Operational monitor + claims analytics |

Because the stack is open source, each layer is visible rather than managed.
[`docs/concepts/databricks-mapping.md`](docs/concepts/databricks-mapping.md) maps every
component to its Databricks equivalent — Auto Loader, Unity Catalog, Workflows, Delta Live
Tables, job clusters — since the managed platform is running these same technologies.

---

## Running it

```bash
cp .env.example .env          # fill in local credentials
docker compose up -d          # MinIO, PostgreSQL, Spark, Airflow, Trino, Streamlit
python scripts/init_minio.py  # create buckets
python scripts/file_watcher.py
```

Drop a file matching the naming convention into `data/inbound/` and the pipeline triggers.
Full walkthrough and service URLs in [`run.md`](run.md).

---

## Documentation

- [`docs/PROJECT_SUMMARY.md`](docs/PROJECT_SUMMARY.md) — full architecture, schemas, roadmap
- [`docs/architecture/decisions.md`](docs/architecture/decisions.md) — ADRs
- [`docs/concepts/`](docs/concepts/) — medallion architecture, Delta Lake, PHI masking,
  file partitioning, quarantine handling, Trino, Databricks mapping
