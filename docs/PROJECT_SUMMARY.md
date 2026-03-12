# Healthcare Data Pipeline — Project Summary

## 🎯 Project Goal
Build a **healthcare claims & billing data pipeline** end-to-end using industry standard tools — to learn and showcase full data engineering skills independently, without relying on IT or platform teams.

---

## 🏗️ Architecture

```
Claims Sources → Ingestion (Kafka) → Storage (MinIO) → Processing (Spark) → Orchestration (Airflow) → Analytics
```

**Data Formats:** X12 EDI 837P/837I (claims), 835 (remittance)

**Processing Pattern:** Micro-batch (sweet spot for claims data)

---

## 💻 Hardware & Environment
```
Machine:    Mac Mini M4 2024
RAM:        16GB (limiting factor)
Storage:    512GB SSD (plenty)
OS:         macOS 26
Strategy:   Lightweight local stack + cloud for scale later
```

---

## 🛠️ Tech Stack

| Layer | Tool | Why |
|---|---|---|
| Ingestion | Kafka (optional) | Real-time claims feeds |
| Storage | MinIO | Lightweight HDFS replacement, S3-compatible |
| Processing | Spark 3.5.1 (single container) | Same as production, local mode |
| Orchestration | Airflow 2.9.1 | Industry standard pipeline scheduler |
| Metadata | PostgreSQL 16 | Airflow DB + audit logs + reference data |
| Containerization | Docker Desktop | Runs all services, easy reset |
| Version Control | Git | Tracks every change from day one |
| DB GUI | TablePlus | Visual PostgreSQL management |

---

## 📁 Project Structure
```
healthcare-pipeline/
├── ingestion/          ← Kafka producer code, intake logic
├── processing/
│   ├── scala/          ← Spark Scala jobs
│   └── pyspark/        ← PySpark jobs
├── orchestration/      ← Airflow DAGs
├── infra/
│   ├── minio/
│   ├── postgres/
│   ├── spark/
│   └── airflow/
├── config/             ← app/env/yaml configs
├── scripts/            ← shell scripts
├── docs/               ← architecture notes
├── tests/              ← unit/integration tests
└── data/
    ├── raw/            ← Stage 0: as-is from source
    ├── cleansed/       ← Stage 1: validated, deduped, masked
    ├── transformed/    ← Stage 2: business rules applied
    ├── curated/        ← Stage 3: reporting ready
    ├── quarantine/     ← rejected/failed records
    └── reference/      ← lookup tables
```

---

## 🐳 Docker Services

```
Service             Port        Status
───────             ────        ──────
MinIO               9001        ✅ healthy
PostgreSQL          5432        ✅ healthy
Spark               8080        ✅ running
Airflow Webserver   8082        ✅ running
Airflow Scheduler   -           ✅ running
```

**Docker memory limit:** 8GB (half of 16GB — golden rule)

---

## 🌐 Service URLs
```
MinIO UI      → http://localhost:9001  (minioadmin / minioadmin123)
Spark UI      → http://localhost:8080
Airflow UI    → http://localhost:8082  (admin / admin123)
PostgreSQL    → localhost:5432         (pgadmin / pgpassword123)
```

---

## 🪣 MinIO Buckets (Data Lake Zones)
```
healthcare-raw          ← Stage 0
healthcare-cleansed     ← Stage 1
healthcare-transformed  ← Stage 2
healthcare-curated      ← Stage 3
healthcare-quarantine   ← failed records
healthcare-reference    ← lookup tables
```

---

## 🗄️ PostgreSQL Databases & Tables

**Databases:**
```
pipeline_db    ← pipeline metadata and audit
airflow_db     ← Airflow internals
reference_db   ← CPT codes, ICD10, payer data
```

**Tables in pipeline_db:**
```sql
source_system    ← tracks data origins
                   (source_id, source_name, source_type, active_flag)

file_registry    ← tracks every file entering pipeline
                   (file_id, source_id, file_name, bucket_name,
                    object_key, file_hash, ingestion_status)
                   status: RECEIVED → PROCESSING → CLEANSED
                         → TRANSFORMED → CURATED → FAILED → QUARANTINED

pipeline_run     ← tracks every Spark job execution
                   (run_id, pipeline_name, run_status,
                    records_read, records_written, records_rejected,
                    error_message)
                   status: RUNNING → SUCCESS → FAILED → PARTIAL
```

---

## 🔑 Key Concepts

**Docker Compose:**
```
image      → blueprint for container
ports      → doors between Mac and container
volumes    → persistent storage (survives restarts)
networks   → how containers talk to each other
depends_on → service startup order
```

**Data Lake Zones (Medallion Architecture):**
```
Raw → Cleansed → Transformed → Curated
Each zone = one Spark job writing to disk
Never process in memory between pipeline stages
```

**MinIO vs HDFS:**
```
MinIO   = S3-compatible, lightweight, single container
HDFS    = needs full Hadoop cluster, heavy RAM
Code    = identical (both use S3A connector in Spark)
Migration to cloud = just change the URL
```

**Spark Local Mode:**
```
--master local[*]  = uses all Mac CPU cores
No YARN needed     = no cluster overhead
spark-submit       = same command as production
4040 port          = only active when job running
```

**Airflow Components:**
```
airflow-init       → one-time DB setup, then exits
airflow-webserver  → UI at localhost:8082
airflow-scheduler  → watches schedules, triggers DAGs
DAG                → your pipeline definition
Task               → one step in the pipeline
Operator           → type of task (Spark, Python, Bash, SQL)
```

---

## 🔧 Useful Commands Reference

```bash
# Docker
docker-compose up -d <service>     # start a service
docker-compose down                # stop everything
docker-compose down -v             # stop + delete all data
docker ps                          # see running containers
docker logs -f <container>         # watch container logs
docker exec -it <container> bash   # go inside container

# PostgreSQL
docker exec -it postgres psql -U pgadmin -d pipeline_db

# Git
git init                           # initialize repo
git add .                          # stage all changes
git commit -m "message"            # save snapshot
git log --oneline                  # see commit history
git status                         # check current state

# Homebrew
brew install --cask <app>          # install Mac app
brew install <tool>                # install CLI tool
```

---

## 🎯 Interview Points

```
1. "I designed and built a full local data engineering stack from scratch"
2. "I implemented the medallion architecture with zone-based data lake"
3. "I containerized all services using Docker Compose"
4. "I used MinIO as S3-compatible local storage — zero code changes to go to cloud"
5. "I designed metadata tables for data lineage and pipeline audit tracking"
6. "I set up Airflow with PostgreSQL backend for pipeline orchestration"
7. "I run Spark in local mode for development — same spark-submit as production"
```

---

## ⬜ What's Next
```
⬜ Write first Spark job (PySpark)
⬜ Write first Airflow DAG
⬜ Build claims data producer
⬜ Build ingestion pipeline
⬜ Build ETL pipeline (cleanse → transform → curate)
⬜ End to end run
⬜ Add Kafka (ingestion layer)
⬜ Add fraud detection (Spark MLlib)
```

## 📝 Future Enhancements
- [ ] Dynamic file routing based on date in filename (e.g. claims_20260312.csv)
- [ ] INPUT_PATH and OUTPUT_PATH derived from filename date
- [ ] file_registry table already supports this via file_name and object_key columns
- [ ] Filename convention: {CLIENT_CODE}_{FILE_TYPE}_{DATE}.csv
      e.g. BCBS001_837P_20260312.csv
- [ ] Parse filename to extract: client_code, file_type, date
- [ ] client_code → lookup source_system table → get source_id
- [ ] file_type → route to correct Spark processing logic
- [ ] date → route to correct MinIO partition
