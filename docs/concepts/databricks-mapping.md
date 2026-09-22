# Databricks Mapping — Local Stack to Databricks

> **Built for real:** the Azure half of this mapping now exists as working code —
> [github.com/bhumadhi/datadissect-azure](https://github.com/bhumadhi/datadissect-azure).
> ADLS Gen2, managed identity, Unity Catalog and the claims pipeline, provisioned
> in Terraform. The corrections below (ABFS vs S3A especially) come from building
> it rather than reading about it.

## The core point

DataDissect is built on open-source tools so every layer is visible and understandable without abstraction. Databricks is a managed platform that runs the same underlying technologies — Delta Lake, Apache Spark, the Medallion architecture — but handles infrastructure, optimization, and governance for you.

**Understanding the open-source layer makes you a stronger Databricks engineer** — you know what Databricks is doing under the hood, not just how to click buttons in the UI.

---

## Full Stack Mapping

| DataDissect (local) | Databricks equivalent | What changes |
|---|---|---|
| MinIO (object store) | S3 / ADLS Gen2 / GCS (external storage) | **Different driver per cloud** — `s3a://` for S3, `abfss://` for ADLS Gen2 |
| Apache Spark `local[*]` | Databricks Runtime (managed Spark) | Databricks adds Photon engine, cluster auto-scaling |
| Delta Lake 3.0.0 | Delta Lake (built-in) | Same format — Databricks invented Delta Lake |
| Airflow DAG | Databricks Workflows / Jobs | Declarative JSON job definitions, native Spark integration |
| Trino | Databricks SQL (DBSQL) | SQL Warehouse (serverless or provisioned), same SQL dialect |
| File-based metastore | Unity Catalog | Centralized governance — catalog.schema.table addressing |
| File watcher + REST API | Auto Loader (`cloudFiles`) | Native Spark Structured Streaming for file ingestion |
| Streamlit dashboard | Databricks SQL Dashboards / Lakeview | Built into the platform, queries DBSQL directly |
| Docker Compose | Databricks workspace + cluster config | Databricks manages all infrastructure |
| `.env` credentials | Databricks Secrets (+ Azure KV / AWS Secrets Manager) | Secret scopes, never in code |
| `pipeline_db` (PostgreSQL audit) | Delta table in Unity Catalog | Audit data becomes a first-class Delta table |
| `spark-submit` | `databricks jobs run-now` / `dbx` CLI | Job clusters spin up, run, terminate automatically |
| `processing/common/path_utils.py` | Unity Catalog table names | No path construction — tables addressed as `catalog.schema.table` |
| Medallion architecture | Same — Databricks calls it the same thing | Bronze/Silver/Gold or Raw/Cleansed/Curated |

---

## Component Deep Dives

### MinIO → External Storage (S3 / ADLS Gen2 / GCS)

**Local:** MinIO runs in Docker. Spark connects via the S3A connector (`hadoop-aws`) with `path.style.access=true` and an explicit endpoint.

**On Azure, it is not S3A.** ADLS Gen2 uses the **ABFS** driver (`hadoop-azure`), and paths look like:

```
abfss://<container>@<account>.dfs.core.windows.net/<path>
```

`s3a://` is S3 only. These are two different implementations of the Hadoop FileSystem API, not aliases for each other. Pointing `s3a://` at an ADLS Gen2 account does not work.

Two details that matter in practice:

- **`dfs` vs `blob` endpoint.** A storage account with hierarchical namespace enabled exposes *both* `…dfs.core.windows.net` (filesystem API: real directories, atomic rename) and `…blob.core.windows.net` (flat object API) over the same bytes. ABFS talks to `dfs`. Without hierarchical namespace you only get the flat one, and renaming a directory becomes a copy of every object — which is why Spark's commit protocol is slow and non-atomic on flat blob storage.

- **No key in the code.** The cluster authenticates with a managed identity; Unity Catalog holds a *storage credential* wrapping that identity and an *external location* binding it to a path. Access is a `GRANT`, not a secret.

**Interview:** *"Locally I configure S3A by hand — endpoint, access key, path-style access. On Azure it's a different driver entirely: ABFS, `abfss://`, against the dfs endpoint. And there's no key anywhere — the cluster's managed identity gets a token from the platform and Unity Catalog checks the grant on the external location."*

---

### Spark `local[*]` → Databricks Runtime

**Local:** Spark runs on one machine using all CPU cores. No fault tolerance. Memory limited to Mac Mini RAM.

**Databricks Runtime (DBR):** Managed Spark on a cluster of cloud VMs. DBR adds:
- **Photon engine** — C++ vectorized query engine that replaces the Spark JVM execution engine for SQL and DataFrame operations. 2–10x faster on analytical workloads.
- **Auto-scaling** — cluster adds/removes workers based on load
- **Cluster types:**
  - **All-purpose cluster** — interactive, long-running, used in notebooks
  - **Job cluster** — ephemeral, spins up for a job run then terminates. Cheaper.
  - **SQL Warehouse** — for DBSQL queries only, serverless or provisioned

**Interview:** *"My local Spark runs in local[*] mode — same code, one machine. Databricks Runtime is the same Spark engine with Photon on top for vectorized execution and cluster management for horizontal scale. The PySpark code is identical."*

---

### File Watcher → Auto Loader

**Local:** `file_watcher.py` uses `watchdog` to detect new files in `data/inbound/`, validates the filename, uploads to MinIO, and triggers Airflow via REST API. Custom code — ~200 lines.

**Databricks Auto Loader:** `spark.readStream.format("cloudFiles")` — a built-in Spark Structured Streaming source that:
- Watches an S3/ADLS/GCS path for new files continuously
- Tracks which files have been processed in a **checkpoint** (no duplicate processing)
- Scales to millions of files without listing the entire bucket each time (uses cloud event notifications — S3 SQS, Azure Event Grid)
- Auto-detects schema from incoming files

```python
# Auto Loader in Databricks
df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", "/mnt/checkpoints/claims/schema")
    .load("s3://healthcare-raw/")
)

df.writeStream
  .format("delta")
  .option("checkpointLocation", "/mnt/checkpoints/claims/")
  .table("bronze.claims_raw")
```

**What you replace:** The entire `file_watcher.py` (watchdog, upload logic, DAG trigger) becomes ~10 lines of Auto Loader code. The pipeline becomes a continuous stream rather than event-triggered batch.

**Interview:** *"My file watcher is a custom Python watchdog implementation. In Databricks, Auto Loader replaces that entirely — it's a native Structured Streaming source that watches cloud storage, tracks state in a checkpoint, and handles exactly-once semantics automatically. My implementation taught me what Auto Loader is solving under the hood."*

---

### Airflow → Databricks Workflows

**Local:** Airflow DAG defined in Python. 7 tasks. `schedule=None`. Triggered via REST API.

**Databricks Workflows:** Job definitions in JSON or YAML (or UI). Each task can be a notebook, Python script, JAR, SQL query, or dbt project. Dependencies defined the same way.

```json
{
  "name": "claims_pipeline",
  "tasks": [
    {
      "task_key": "cleanse",
      "python_wheel_task": {"package_name": "claims", "entry_point": "cleanse"},
      "job_cluster_key": "claims_cluster"
    },
    {
      "task_key": "transform",
      "depends_on": [{"task_key": "cleanse"}],
      "python_wheel_task": {"package_name": "claims", "entry_point": "transform"},
      "job_cluster_key": "claims_cluster"
    }
  ]
}
```

**Key difference:** Databricks Workflows creates a **job cluster** per run — the cluster spins up, runs the tasks, then terminates. No always-on scheduler container. Costs only for actual execution time.

**Interview:** *"My Airflow DAG is a 7-task linear pipeline triggered via REST API. In Databricks, this maps to a Workflow with job clusters — ephemeral clusters that spin up per run and terminate when done. The task dependency model is the same; Databricks adds native Spark integration and eliminates the Airflow infrastructure overhead."*

---

### Trino → Databricks SQL (DBSQL)

**Local:** Trino 435 in Docker. Two catalogs: Delta (MinIO) and PostgreSQL. Cross-source SQL.

**Databricks SQL:** SQL Warehouses (serverless or provisioned) that query Delta tables registered in Unity Catalog. ANSI SQL, same Delta connector. Native Photon acceleration.

```sql
-- DBSQL — same SQL, tables addressed via Unity Catalog
SELECT
    m.member_id_hash,
    m.total_claims,
    m.total_billed,
    p.run_status,
    p.records_written
FROM datadissect.curated.member_summary m
JOIN datadissect.audit.pipeline_run p
    ON p.file_name = m.source_file
ORDER BY m.total_billed DESC;
```

**Key difference:** In Trino, tables are registered manually via `CALL delta.system.register_table()`. In Unity Catalog, tables are registered once and governed centrally — access control, lineage, and auditing are automatic.

---

### File-based Metastore → Unity Catalog

**Local:** Trino uses a file-based metastore stored in `s3a://healthcare-metadata/trino-catalog/`. Tables registered manually per run.

**Unity Catalog (UC):** Databricks' centralized governance layer. Three-level namespace: `catalog.schema.table`.
- **Data catalog** — all tables, views, volumes discoverable in one place
- **Fine-grained access control** — `GRANT SELECT ON TABLE datadissect.curated.member_summary TO GROUP analysts`
- **Data lineage** — UC automatically tracks which notebook/job wrote to which table
- **Audit logs** — every read and write logged automatically
- **Column-level security** — mask PHI columns for certain users at the catalog level

**Interview:** *"My local stack uses a file-based Trino metastore — I register tables manually. Unity Catalog replaces that with centralized governance: one place for table discovery, access control, lineage, and audit logs. Column-level security in UC is where PHI masking shifts from a Spark job concern to a catalog-level policy."*

---

### Delta Live Tables (DLT) — No local equivalent

DLT is a Databricks-native declarative pipeline framework. Instead of writing imperative Spark jobs that read from one path and write to another, you declare tables:

```python
import dlt

@dlt.table(comment="Raw claims from inbound files")
def bronze_claims():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .load("s3://healthcare-raw/")
    )

@dlt.table(comment="Validated and PHI-masked claims")
@dlt.expect_all_or_drop({
    "valid_claim_id": "claim_id IS NOT NULL",
    "valid_amount": "billed_amount > 0"
})
def silver_claims():
    return (
        dlt.read_stream("bronze_claims")
        .withColumn("member_id_hash", sha2(col("member_id"), 256))
        .drop("member_id", "provider_npi")
    )

@dlt.table(comment="Member-level curated summaries")
def gold_member_summary():
    return (
        dlt.read("silver_claims")
        .groupBy("member_id_hash")
        .agg(count("*").alias("total_claims"), sum("billed_amount").alias("total_billed"))
    )
```

DLT handles: dependency ordering, retries, data quality enforcement (`expect`), incremental processing, lineage visualization — automatically.

**The relationship to DataDissect:** `claims_cleanse.py` → bronze, `claims_transform.py` → silver, `claims_curate.py` → gold. DLT is the Databricks-managed version of the same Medallion pattern.

---

## The Interview Answer

**Q: You built this on open-source tools — how does this apply to Databricks?**

> "The architecture is identical — Databricks is built on the same open-source stack. Delta Lake is a Databricks invention, the Medallion pattern is what Databricks evangelizes, and Spark is the same engine. The difference is abstraction and management.
>
> My file watcher maps to Auto Loader — Databricks' native cloudFiles source that watches object storage and handles exactly-once ingestion. My Airflow DAG maps to Databricks Workflows — same task dependency model, but with ephemeral job clusters instead of an always-on scheduler. My Trino query layer maps to Databricks SQL with Unity Catalog — same SQL on Delta tables, but with centralized governance, lineage, and access control built in.
>
> Building on open-source first means I understand what each Databricks feature is solving. When Auto Loader mentions checkpointing, I know what state it's tracking. When Unity Catalog shows lineage, I know which job wrote which table. That understanding doesn't come from only using the managed platform."

---

## Key Databricks Terms to Know

| Term | What it is |
|---|---|
| **DBR (Databricks Runtime)** | Managed Spark with Photon, pre-installed libraries, security patches |
| **Photon** | C++ vectorized execution engine on top of Spark — faster SQL and DataFrame ops |
| **Auto Loader** | `cloudFiles` Structured Streaming source — watches cloud storage for new files |
| **Unity Catalog** | Centralized governance — catalog.schema.table, access control, lineage, audit |
| **DBSQL / SQL Warehouse** | Managed compute for SQL queries — serverless or provisioned |
| **Delta Live Tables (DLT)** | Declarative Medallion pipeline framework with built-in data quality |
| **Job cluster** | Ephemeral cluster that starts for a job run then terminates |
| **All-purpose cluster** | Long-running interactive cluster for notebooks |
| **DBU (Databricks Unit)** | Billing unit — every cluster hour costs a certain number of DBUs |
| **Databricks Connect** | Run local IDE code against a remote Databricks cluster |
| **MLflow** | Experiment tracking and model registry — built into Databricks |
| **Repos** | Git integration inside Databricks workspace |
| **Volumes** | Unity Catalog-managed unstructured file storage (replaces DBFS paths) |
