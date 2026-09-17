# Run Guide — DataDissect

---

### Terminal 1 — Docker Stack (MinIO · PostgreSQL · Spark · Airflow · Trino)

```bash
cd /Users/bhuwanmadhikarmi/datadissect
docker-compose up -d --build
```

Wait until all services are healthy:

```bash
docker ps
```

| Service           | URL                        | Credentials               |
|-------------------|----------------------------|---------------------------|
| MinIO Console     | http://localhost:9001      | ${MINIO_ACCESS_KEY} / ${MINIO_SECRET_KEY} |
| MinIO API         | http://localhost:9000      | —                         |
| Airflow UI        | http://localhost:8082      | admin / ${AIRFLOW_PASSWORD}          |
| Spark UI          | http://localhost:8080      | —                         |
| Trino             | http://localhost:8083      | —                         |
| PostgreSQL        | localhost:5432             | ${POSTGRES_USER} / ${POSTGRES_PASSWORD}   |

Stop the stack:

```bash
docker-compose down
```

---

### One-time Setup — MinIO Buckets + Reference Data

Run this once after a fresh `docker-compose up -d` (e.g. first time, or after volumes are reset):

```bash
cd /Users/bhuwanmadhikarmi/datadissect
source /Users/bhuwanmadhikarmi/datadissect/.venv/bin/activate
python3 scripts/init_minio.py
```

Also initialize the PostgreSQL schema:

```bash
docker exec -i postgres psql -U ${POSTGRES_USER} -d pipeline_db < scripts/init_db.sql
```

---

### Terminal 2 — File Watcher

Watches `data/inbound/` — validates filenames, uploads to MinIO, triggers Airflow DAG automatically.

```bash
cd /Users/bhuwanmadhikarmi/datadissect
source /Users/bhuwanmadhikarmi/datadissect/.venv/bin/activate
python3 scripts/file_watcher.py
```

Drop a file to trigger the pipeline:

```bash
cp data/reference/cpt_codes.csv data/inbound/BCBS001_837P_PROD_20260312_001.csv
```

---

### Terminal 3 — Streamlit Dashboard

```bash
cd /Users/bhuwanmadhikarmi/datadissect/frontend
source /Users/bhuwanmadhikarmi/datadissect/.venv/bin/activate
streamlit run app.py
```

URL: http://localhost:8501

---

## Utility Commands

### Trigger a DAG manually (no file watcher)

```bash
# Via Airflow CLI inside the container
docker exec airflow-webserver airflow dags trigger claims_pipeline \
  --conf '{"file_name": "BCBS001_837P_PROD_20260312_001.csv"}'

# Via REST API (same as file_watcher.py uses)
curl -X POST "http://localhost:8082/api/v1/dags/claims_pipeline/dagRuns" \
  -H "Content-Type: application/json" \
  -u "admin:${AIRFLOW_PASSWORD}" \
  -d '{"conf": {"file_name": "BCBS001_837P_PROD_20260312_001.csv"}}'
```

### Trino CLI (interactive SQL)

```bash
docker exec -it trino trino
```

### PostgreSQL CLI

```bash
docker exec -it postgres psql -U ${POSTGRES_USER} -d pipeline_db
```

### Recovery tool (reprocess files that arrived while watcher was down)

```bash
cd /Users/bhuwanmadhikarmi/datadissect
source /Users/bhuwanmadhikarmi/datadissect/.venv/bin/activate
python3 scripts/recover_unprocessed.py --dry-run
python3 scripts/recover_unprocessed.py
```

---

## Debugging Guide

Use this section when something breaks. Work top-down: check infrastructure first, then the specific failing stage.

---

### Step 1 — Are all containers running?

```bash
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
```

Expected containers: `minio`, `postgres`, `airflow-webserver`, `airflow-scheduler`, `airflow-init`, `spark`, `trino`

If a container is missing or restarting:

```bash
# See why a container exited
docker logs <container-name> --tail 50

# Rebuild and restart everything
docker-compose down && docker-compose up -d --build
```

---

### Step 2 — Is MinIO healthy and do all buckets exist?

```bash
# Health check — should return HTTP 200
curl -I http://localhost:9000/minio/health/live

# List buckets from inside the Airflow container (same network as Spark jobs)
docker exec airflow-scheduler python3 -c "
from minio import Minio
c = Minio('minio:9000', access_key='${MINIO_ACCESS_KEY}', secret_key='${MINIO_SECRET_KEY}', secure=False)
print([b.name for b in c.list_buckets()])
"
```

Expected buckets: `healthcare-raw`, `healthcare-cleansed`, `healthcare-quarantine`, `healthcare-transformed`, `healthcare-curated`, `healthcare-reference`, `healthcare-scratch`

If buckets are missing (happens after volume reset):

```bash
source /Users/bhuwanmadhikarmi/datadissect/.venv/bin/activate
python3 scripts/init_minio.py
```

List objects inside a specific bucket:

```bash
docker exec airflow-scheduler python3 -c "
from minio import Minio
c = Minio('minio:9000', access_key='${MINIO_ACCESS_KEY}', secret_key='${MINIO_SECRET_KEY}', secure=False)
for obj in c.list_objects('healthcare-raw', recursive=True):
    print(obj.object_name, obj.size)
"
```

---

### Step 3 — Is PostgreSQL reachable and are tables created?

```bash
# Connect and list tables
docker exec -it postgres psql -U ${POSTGRES_USER} -d pipeline_db -c "\dt"
```

Expected tables: `source_system`, `file_registry`, `pipeline_run`

If tables are missing (happens after volume reset):

```bash
docker exec -i postgres psql -U ${POSTGRES_USER} -d pipeline_db < scripts/init_db.sql
```

Check recent pipeline run results:

```bash
docker exec postgres psql -U ${POSTGRES_USER} -d pipeline_db -c \
  "SELECT pipeline_name, run_status, records_read, records_written, records_rejected, started_at
   FROM pipeline_run ORDER BY started_at DESC LIMIT 10;"
```

Check file registry status:

```bash
docker exec postgres psql -U ${POSTGRES_USER} -d pipeline_db -c \
  "SELECT file_name, ingestion_status, received_at, error_message
   FROM file_registry ORDER BY received_at DESC LIMIT 10;"
```

---

### Step 4 — Is Airflow healthy?

```bash
# Scheduler and metastore health
curl -s http://localhost:8082/health | python3 -m json.tool

# List all DAGs and their paused state
curl -s http://localhost:8082/api/v1/dags -u "admin:${AIRFLOW_PASSWORD}" | \
  python3 -c "import sys,json; [print(d['dag_id'], '| paused:', d['is_paused']) for d in json.load(sys.stdin)['dags']]"

# Unpause a DAG
curl -X PATCH http://localhost:8082/api/v1/dags/claims_pipeline \
  -H "Content-Type: application/json" \
  -u "admin:${AIRFLOW_PASSWORD}" \
  -d '{"is_paused": false}'
```

Stream live scheduler logs:

```bash
docker logs airflow-scheduler --tail 50 -f
```

---

### Step 5 — Which Airflow task failed?

Go to http://localhost:8082, find the DAG run, click the red task box, then click **Log**.

Or use the API:

```bash
# List task states for the most recent DAG run
DAG_RUN_ID="manual__2026-05-23T23:34:27.160409+00:00"   # replace with actual run ID

curl -s "http://localhost:8082/api/v1/dags/claims_pipeline/dagRuns/${DAG_RUN_ID}/taskInstances" \
  -u "admin:${AIRFLOW_PASSWORD}" | \
  python3 -c "
import sys, json
for t in json.load(sys.stdin)['task_instances']:
    print(t['task_id'].ljust(35), t['state'])
"
```

Clear a failed task to retry it (from Airflow UI: click the task → **Clear**), or via CLI:

```bash
docker exec airflow-webserver airflow tasks clear claims_pipeline \
  -t run_claims_cleanse \
  --dag-run-id "manual__2026-05-23T23:34:27.160409+00:00" \
  --yes
```

---

### Step 6 — Debug a Spark job directly (bypass Airflow)

Run any stage manually from inside the Airflow container. This gives you the full Spark log in your terminal without navigating the Airflow UI.

**Cleanse:**

```bash
docker exec airflow-scheduler bash -c '
spark-submit \
  --master local[*] \
  --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  /opt/airflow/processing/pyspark/claims_cleanse.py \
  --file-name BCBS001_837P_PROD_20260312_001.csv
'
```

**Transform:**

```bash
docker exec airflow-scheduler bash -c '
spark-submit \
  --master local[*] \
  --packages io.delta:delta-spark_2.12:3.0.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  /opt/airflow/processing/pyspark/claims_transform.py \
  --file-name BCBS001_837P_PROD_20260312_001.csv
'
```

**Curate:**

```bash
docker exec airflow-scheduler bash -c '
spark-submit \
  --master local[*] \
  --packages io.delta:delta-spark_2.12:3.0.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  /opt/airflow/processing/pyspark/claims_curate.py \
  --file-name BCBS001_837P_PROD_20260312_001.csv
'
```

Filter Spark's verbose output to just the lines that matter:

```bash
... 2>&1 | grep -E "(INFO:__main__|ERROR|Exception|Caused by|complete|failed)"
```

---

### Step 7 — Check env vars inside a container

Spark jobs read MinIO and PostgreSQL credentials from env vars. Verify they are what you expect:

```bash
docker exec airflow-scheduler env | grep -E "MINIO|POSTGRES|AIRFLOW"
```

Confirm Spark can reach MinIO from inside the container:

```bash
docker exec airflow-scheduler curl -I http://minio:9000/minio/health/live
```

---

### Common errors and fixes

| Error | What it means | Fix |
|-------|--------------|-----|
| `NoSuchBucket` | Output bucket doesn't exist | `python3 scripts/init_minio.py` |
| `relation "file_registry" does not exist` | DB tables not created | `docker exec -i postgres psql -U ${POSTGRES_USER} -d pipeline_db < scripts/init_db.sql` |
| `FATAL: database "airflow_db" does not exist` | Airflow DB missing after volume reset | `docker exec -it postgres psql -U ${POSTGRES_USER} -d postgres -c "CREATE DATABASE airflow_db;"` |
| `ModuleNotFoundError: No module named 'psycopg2'` | Wrong venv active | `source /Users/bhuwanmadhikarmi/datadissect/.venv/bin/activate` |
| DAG stuck in paused state | DAG is paused | Unpause via UI or `curl -X PATCH .../dags/claims_pipeline -d '{"is_paused": false}'` |
| DAG triggered but no tasks run | DAG still paused, or scheduler not running | Check `docker ps` for `airflow-scheduler`; check `curl http://localhost:8082/health` |
| `Connection refused` on port 8082 | Airflow webserver not running | `docker-compose up -d airflow-webserver` |
