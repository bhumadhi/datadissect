# Architecture Decision Records (ADRs)

Each decision: what was chosen, what the alternatives were, and why. These are the answers to "why did you do it this way?" in interviews.

---

## ADR-001 — Parquet at cleansed, Delta at transform

**Decision:** Write Parquet at the cleansed layer, Delta Lake at transform and curate.

**Alternatives:**
- Delta everywhere — simpler mental model, uniform format across all zones
- Parquet everywhere — no transaction log overhead at any layer

**Why:** Cleansed is ephemeral staging. If something breaks, we re-cleanse from raw — no history needed. Delta's transaction log and checkpoint files add overhead that provides zero value here. Delta starts at transform where data has business value: ACID protects partial writes, time travel lets you audit before/after logic changes, schema enforcement gives downstream consumers a reliable contract.

---

## ADR-002 — schedule=None (event-driven, not time-driven)

**Decision:** Airflow DAG has `schedule=None`. Only triggers via REST API from file watcher.

**Alternatives:**
- `schedule="0 2 * * *"` — run daily at 2am
- `schedule="*/15 * * * *"` — run every 15 minutes and scan for new files

**Why:** Healthcare claims don't arrive on a schedule. A fixed schedule adds unnecessary latency (file waits until next window) or wastes compute (job runs with nothing to process). Each file arrival is an independent event that should trigger an independent pipeline run. Event-driven matches the actual data delivery model.

---

## ADR-003 — BashOperator for spark-submit instead of SparkSubmitOperator

**Decision:** Use `BashOperator` with the full `spark-submit` command inline.

**Alternatives:**
- `SparkSubmitOperator` — Airflow-native operator, abstracts spark-submit into operator parameters
- `SSHOperator` — run spark-submit on a remote machine

**Why:** `BashOperator` puts the exact spark-submit command — with all packages, all `--conf` flags — directly in the task log. When something breaks (wrong package version, missing conf), the full command is visible and copyable from the Airflow UI. `SparkSubmitOperator` abstracts these details behind an Airflow Spark connection, making debugging harder. The additional abstraction provides no value here since Spark runs on the same machine as Airflow.

---

## ADR-004 — SHA-256 hashing for PHI masking

**Decision:** Hash `member_id` and `provider_npi` with SHA-256, drop originals.

**Alternatives:**
- Delete PHI columns entirely — loses the ability to join across files by member/provider
- AES encryption — reversible with a key, but requires key management and rotation
- Tokenization — lookup table maps real ID to token, preserves joins, but requires a secure vault

**Why:** SHA-256 is one-way (cannot be reversed without the original input) and deterministic (same input always produces the same hash). This means cross-file joins still work — a member in the March file and the April file has the same `member_id_hash` in both. Encryption requires key management infrastructure. Tokenization requires a lookup table that itself becomes a PHI liability. For analytics use cases where we never need to recover the original ID, SHA-256 is the right fit. Production adds a secret salt to prevent rainbow table attacks.

---

## ADR-005 — FileMeta frozen dataclass as single source of truth

**Decision:** Parse filename into `FileMeta` once at the entry point of each component. Never pass raw filename strings downstream.

**Alternatives:**
- Pass raw `file_name` string and re-parse as needed
- Pass individual fields (client_code, file_type, env, date, sequence) as separate arguments
- Use a dict

**Why:** A frozen dataclass guarantees the filename was valid at construction time. No downstream component needs to re-validate. Computed properties (`partition`, `stem`, `is_prod`) live next to the data they describe. All path construction flows through path_utils functions that take a `FileMeta` — if zone structure changes, one file changes. Passing raw strings means validation logic is duplicated or skipped, and it's easy to pass arguments in the wrong order.

---

## ADR-006 — MinIO as system of record, delete local after upload

**Decision:** After uploading a file to MinIO, delete the local copy in `data/inbound/`.

**Alternatives:**
- Keep local copies as a backup — two sources of truth
- Move to `data/processed/` — local archive alongside MinIO

**Why:** Two copies means two sources of truth — they will eventually diverge. MinIO is the authoritative store for all pipeline data. Local `data/inbound/` is a transient landing pad. Deleting after upload enforces this: the file watcher's job is done once MinIO has the file. Keeping local copies adds storage cost and confusion about which copy is authoritative.

---

## ADR-007 — Validate between every pipeline stage (fail-fast)

**Decision:** After each Spark job, a validation task checks that output files exist in MinIO before the next job runs.

**Alternatives:**
- Validate only at the end — simpler DAG, but failures are detected late
- Trust the Spark job's exit code — if spark-submit exits 0, assume output is correct

**Why:** A Spark job can exit successfully but write to the wrong path, write zero records (all quarantined), or write corrupt Parquet. Validating between stages catches these before downstream jobs run against bad input. A transform job that runs on empty cleansed output writes empty Delta tables — a silent failure that's hard to trace. Fail-fast at each boundary makes debugging straightforward: the error is at the stage where data quality broke.

---

## ADR-008 — Partition path: CLIENT/FILE_TYPE/ENV/DATE_SEQ

**Decision:** Partition all MinIO zones by `{CLIENT}/{FILE_TYPE}/{ENV}/{DATE}_{SEQUENCE}`.

**Alternatives:**
- Flat: `{CLIENT}_{FILE_TYPE}_{ENV}_{DATE}_{SEQUENCE}/` — no predicate pushdown
- Date-first: `{DATE}/{CLIENT}/{FILE_TYPE}/` — optimizes date-range queries, not client queries
- Hive-style: `client=BCBS001/file_type=837P/env=PROD/` — Spark reads partition values automatically

**Why:** The primary query pattern is "all files for a given client and file type" — `BCBS001/837P/PROD/*/` matches all dates for that client/type. Date-first partitioning would require scanning all clients to find BCBS001's data. DATE and SEQUENCE are kept combined as the leaf (`20260312_001`) because they're always queried as a unit — a file is identified by date + sequence together.

---

## ADR-009 — Docker LocalExecutor instead of CeleryExecutor

**Decision:** Airflow runs with `LocalExecutor` — tasks execute as subprocesses on the Airflow container.

**Alternatives:**
- `CeleryExecutor` — distributes tasks across worker nodes via a message broker (Redis/RabbitMQ)
- `KubernetesExecutor` — each task runs in an isolated pod

**Why:** Single-machine development environment. `LocalExecutor` runs tasks as subprocesses — simple, no additional services required. `CeleryExecutor` requires a Redis or RabbitMQ broker and separate worker containers — adds operational complexity without benefit on a single machine. In production on a real cluster, `CeleryExecutor` or `KubernetesExecutor` would be appropriate for horizontal scaling.

---

## ADR-010 — Trino file-based metastore instead of Hive Metastore

**Decision:** Trino Delta connector uses `hive.metastore=file` with `s3a://healthcare-metadata/trino-catalog/` as the catalog directory.

**Alternatives:**
- Hive Metastore Service (HMS) — production standard, separate service, requires more setup
- AWS Glue — managed metastore, ties to AWS

**Why:** Running a full Hive Metastore Service requires additional Docker containers (HMS + MySQL/PostgreSQL backing store). File-based metastore stores table metadata as JSON files in the object store — simpler, zero additional services, adequate for single-node Trino. In production on AWS, Glue or HMS would be the right choice.
