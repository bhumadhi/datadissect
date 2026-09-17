# Apache Airflow & DAG Design

## What is it

Apache Airflow is a workflow orchestration platform. You define pipelines as **DAGs** (Directed Acyclic Graphs) — Python code that describes tasks and their dependencies. Airflow handles scheduling, execution, retries, logging, and a UI for monitoring.

**Key concepts:**

- **DAG** — a collection of tasks with defined dependencies. Acyclic = no loops. A DAG defines *what* runs, not *how*.
- **Operator** — the unit of work. `BashOperator` runs a shell command. `PythonOperator` calls a Python function. `SparkSubmitOperator`, `HttpOperator`, etc. exist for specific systems.
- **Task** — an instance of an operator inside a DAG. Each task produces a log, tracks state (queued → running → success/failed), and can retry independently.
- **DAG Run** — one execution of the DAG. Can be triggered manually, by schedule, or by the REST API. Each run has a unique `run_id` and can carry configuration via `conf`.
- **XCom** — cross-task communication. A task can push a value (`xcom_push`) and another task can pull it (`xcom_pull`). Used for passing small values — not dataframes.
- **Executor** — `LocalExecutor` runs tasks as subprocesses on the same machine (what we use). `CeleryExecutor` distributes tasks across workers. `KubernetesExecutor` launches each task in a pod.

---

## Why we use it here

Claims pipeline has strict sequential dependencies: you can't transform data that hasn't been cleansed, and you can't validate cleansed output that hasn't been written yet. Airflow enforces these dependencies, provides retries when transient failures occur (Spark startup latency, network blips), logs every task execution for debugging, and gives a UI to monitor pipeline health across all runs.

Alternatives considered:
- **Prefect/Dagster** — newer, Python-native, better for complex dynamic graphs. Airflow wins on industry adoption — it's what most healthcare/finance shops run.
- **Cron + shell scripts** — no retry logic, no UI, no dependency management, no audit trail.

---

## How it's implemented

**DAG definition:**

```python
with DAG(
    dag_id="claims_pipeline",
    default_args={
        "owner": "data-engineering",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "email_on_failure": False,
    },
    schedule=None,        # event-driven only
    start_date=datetime(2026, 3, 13, tzinfo=timezone.utc),
    catchup=False,
    tags=["healthcare", "claims", "pipeline"],
) as dag:
```

`start_date` is required by Airflow even for `schedule=None` DAGs — it's a reference point, not the first run time.

**7-task pipeline flow:**

```
check_file_exists          ← PythonOperator
        ↓
run_claims_cleanse         ← BashOperator (spark-submit)
        ↓
validate_cleansed_output   ← PythonOperator
        ↓
run_claims_transform       ← BashOperator (spark-submit)
        ↓
validate_transformed_output ← PythonOperator
        ↓
run_claims_curate          ← BashOperator (spark-submit)
        ↓
validate_curated_output    ← PythonOperator
```

Dependencies set with `>>` operator:
```python
(
    check_file_task
    >> run_cleanse_task
    >> validate_cleansed_task
    >> run_transform_task
    >> validate_transformed_task
    >> run_curate_task
    >> validate_curated_task
)
```

**Why BashOperator for Spark, not SparkSubmitOperator?**

`SparkSubmitOperator` requires a Spark connection configured in Airflow's connection store and a `spark-submit` binary on PATH. `BashOperator` directly calls `spark-submit` from the shell — simpler, more transparent, easier to debug (the exact command is in the task log), and avoids the Airflow connection abstraction.

**The `get_meta()` pattern:**

Every Python task re-parses `FileMeta` from `dag_run.conf` at the start:

```python
def get_meta(**context) -> FileMeta:
    file_name = context["dag_run"].conf.get("file_name")
    if not file_name:
        raise ValueError("dag_run.conf['file_name'] is required.")
    return parse_filename(file_name)
```

This is intentional. XCom could pass the filename between tasks, but that creates hidden dependencies. Each task being self-contained means you can re-run any single task in isolation (Airflow's "Clear Task" feature) without the previous task having to have run first in the same session.

**Fail-fast validation pattern:**

Each Spark job is followed by a validation task that checks MinIO for output before the next job runs:

```python
def validate_cleansed_output(**context) -> int:
    meta = get_meta(**context)
    prefix = f"claims/{meta.partition}/"
    count = check_parquet_files(client, CLEANSED_BUCKET, prefix)  # raises if 0 files
    update_file_registry(conn, "CLEANSED", meta.file_name)
    return count
```

`check_parquet_files` raises an exception if no parquet files exist under the prefix. That failure stops the DAG — transform never runs on missing cleansed data.

**`update_file_registry` DRY helper:**

Instead of duplicating the PostgreSQL UPDATE in every validate task:

```python
def update_file_registry(conn, status: str, file_name: str, error_msg=None) -> None:
    cursor.execute(
        "UPDATE file_registry SET ingestion_status = %s, processed_at = NOW(), error_message = %s "
        "WHERE bucket_name = %s AND object_key = %s",
        (status, error_msg, RAW_BUCKET, file_name),
    )
    conn.commit()
```

Every validate task calls this with the appropriate status: `CLEANSED`, `TRANSFORMED`, `CURATED`, or `FAILED`.

**Triggering via REST API:**

```bash
curl -u admin:${AIRFLOW_PASSWORD} \
  -X POST http://localhost:8082/api/v1/dags/claims_pipeline/dagRuns \
  -H "Content-Type: application/json" \
  -d '{"conf": {"file_name": "BCBS001_837P_PROD_20260312_001.csv"}}'
```

Requires `AIRFLOW__API__AUTH_BACKENDS=airflow.api.auth.backend.basic_auth` in the Airflow environment — without this, the REST API returns 403.

---

## Interview Q&A

**Q: Walk me through your DAG design.**

> It's a 7-task linear pipeline: check file exists in MinIO, cleanse, validate cleansed output, transform, validate transformed output, curate, validate curated output. Spark jobs run via BashOperator — the exact spark-submit command is in the task log, making debugging straightforward. Between every Spark job there's a Python validation task that checks MinIO for output files and updates the file_registry audit table. If any stage fails, the DAG stops — downstream stages never run on bad data.

**Q: Why validate between every stage instead of just at the end?**

> Fail fast. If the cleanse job runs successfully but writes to the wrong path — or the output is empty because all records were quarantined — I want to know before the transform job runs against empty input. Without intermediate validation, transform succeeds on an empty dataset and writes empty Delta tables. That failure is much harder to debug than "validate_cleansed_output found 0 parquet files."

**Q: Why not use SparkSubmitOperator?**

> BashOperator is more transparent — the exact spark-submit command with all packages and configs appears verbatim in the task log. SparkSubmitOperator abstracts those details behind an Airflow connection, which makes debugging harder when something goes wrong with Spark startup or package resolution.

**Q: How do you pass data between tasks?**

> The `file_name` is in `dag_run.conf` — the configuration payload passed when the DAG is triggered. Every task re-reads it via `context["dag_run"].conf`. I deliberately don't use XCom for this because it makes each task self-contained — you can clear and re-run any individual task in Airflow without needing the previous task to have run first in the same session.

**Q: What happens if a task fails?**

> The task retries up to 2 times with a 5-minute delay between attempts — handles transient failures like Spark startup lag or a brief network issue. If all retries fail, the DAG run is marked FAILED, downstream tasks are skipped, and `file_registry` is updated to FAILED with the error message. The file stays in MinIO raw — nothing is lost. You can fix the issue and re-trigger the DAG.
