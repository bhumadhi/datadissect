# Concepts Index

One file per concept. Each file covers: what it is (theory), why we use it, how it's implemented in DataDissect, and interview Q&A.

Read these before interviews. As new phases are built, add a new file the same day.

---

## Phase 1–7 (Complete)

| Concept | File | One-line summary |
|---|---|---|
| Medallion Architecture | [medallion-architecture.md](medallion-architecture.md) | Raw → Cleansed → Transformed → Curated — why each zone exists and what format it uses |
| Delta Lake | [delta-lake.md](delta-lake.md) | ACID + time travel on Parquet via transaction log — when to use it vs plain Parquet |
| Event-Driven Pipeline | [event-driven-pipeline.md](event-driven-pipeline.md) | File drop triggers pipeline via watchdog + Airflow REST API — why not scheduled |
| Airflow DAG Design | [airflow-dag.md](airflow-dag.md) | 7-task fail-fast DAG — BashOperator vs PythonOperator, retries, validation pattern |
| Spark + MinIO | [spark-minio.md](spark-minio.md) | S3A connector, hadoop-aws packages, path style access, lazy evaluation |
| File Partitioning | [file-partitioning.md](file-partitioning.md) | CLIENT/FILE_TYPE/ENV/DATE_SEQ paths — predicate pushdown, small files, zone map |
| PHI Masking | [phi-masking.md](phi-masking.md) | SHA-256 hashing — HIPAA, deterministic joins, rainbow table defense |
| Trino | [trino.md](trino.md) | Federated SQL engine — catalogs, cross-source queries, Delta + PostgreSQL in one query |
| File Quarantine | [file-quarantine.md](file-quarantine.md) | Dead letter queue pattern — file-level vs record-level, fallback to local hold |
| FileMeta Pattern | [filemeta-pattern.md](filemeta-pattern.md) | Parse once at boundary, typed immutable object, single source of truth for all paths |

---

## Upcoming (add as built)

| Phase | Concept to document |
|---|---|
| Phase 8 | Plugin architecture, client config YAML, delimiter auto-detection, header/trailer validation |
| Phase 9 | Restart-from-failed-stage, Airflow clear task API |
| Phase 10 | Scala Spark — JVM vs Python, SBT, case classes as FileMeta equivalent |
| Phase 11 | Architecture Decision Records (ADRs) |
| Phase 12 | Kafka — producers, consumers, partitions, consumer groups, offset management |
| Future | CDC (Debezium), dbt, Great Expectations, Terraform, FastAPI, Iceberg |
