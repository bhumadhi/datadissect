# Trino (Distributed SQL Query Engine)

## What is it

Trino (formerly PrestoSQL) is a distributed SQL query engine. The critical distinction:

**Trino is compute, not storage.** It does not store data. It connects to existing data sources via **connectors**, translates SQL into federated queries across those sources, and returns results. When you query Trino, it reaches into MinIO, PostgreSQL, Hive, Kafka, or any connected system — retrieves the relevant data — joins and aggregates it in memory — and returns results.

**Key properties:**

- **MPP (Massively Parallel Processing)** — queries are split into tasks distributed across Trino worker nodes, each processing a slice of data in parallel
- **Separation of compute and storage** — Trino workers can scale independently of storage
- **Federation** — a single SQL query can join a Delta table in MinIO with a PostgreSQL table. No ETL needed to combine them.
- **ANSI SQL** — standard SQL, not a dialect. Works with any BI tool (Tableau, Superset, Metabase) via JDBC/ODBC
- **No data movement for reads** — Trino reads data where it lives. No copying to a central store

**How Trino connects to data — Catalogs and Connectors:**

```
Trino
  └── Catalog: delta      → Delta connector → MinIO S3A
  └── Catalog: postgresql → PostgreSQL connector → pipeline_db
  └── Catalog: system     → built-in system tables
```

A catalog is a named configuration pointing at a connector. Tables are addressed as `catalog.schema.table`.

---

## Why we use it here

After the pipeline runs, curated Delta tables live in MinIO and audit tables live in PostgreSQL. Without Trino, you'd need to either:
- Write separate Python code to query each system and join in pandas (slow, manual)
- ETL everything into one system (data duplication, extra pipeline step)

Trino lets us write one SQL query that joins Delta tables from MinIO with `file_registry` from PostgreSQL — with no data movement. The Streamlit dashboard uses this for its cross-source lineage view: "show me file metadata alongside pipeline run metrics for the same file."

---

## How it's implemented

**Configuration files (`infra/trino/`):**

`config.properties` — server settings:
```properties
coordinator=true
node-scheduler.include-coordinator=true
http-server.http.port=8080
query.max-memory=1GB
query.max-memory-per-node=1GB
discovery.uri=http://localhost:8080
```

`jvm.config` — JVM heap:
```
-Xmx1536M
```
Set to 1.5GB. Trino's query memory is managed separately from JVM heap — JVM heap covers Trino internals (coordinator, metadata). Keep JVM heap smaller than total container memory (Docker 8GB limit).

`catalog/delta.properties` — Delta connector:
```properties
connector.name=delta_lake
hive.metastore=file
hive.metastore.catalog.dir=s3a://healthcare-metadata/trino-catalog
hive.s3.endpoint=http://minio:9000
hive.s3.path-style-access=true
hive.s3.aws-access-key=${MINIO_ACCESS_KEY}
hive.s3.aws-secret-key=${MINIO_SECRET_KEY}
delta.register-table-procedure.enabled=true
```

`delta.register-table-procedure.enabled=true` is required to use `CALL delta.system.register_table(...)` — without it, you get a "procedure not found" error.

`catalog/postgresql.properties` — PostgreSQL connector:
```properties
connector.name=postgresql
connection-url=jdbc:postgresql://postgres:5432/pipeline_db
connection-user=${POSTGRES_USER}
connection-password=${POSTGRES_PASSWORD}
```

**Registering Delta tables (run once after pipeline produces output):**

```sql
CREATE SCHEMA IF NOT EXISTS delta.healthcare
WITH (location = 's3a://healthcare-curated/');

CALL delta.system.register_table(
    schema_name    => 'healthcare',
    table_name     => 'member_summary',
    table_location => 's3a://healthcare-curated/member_summary/BCBS001/837P/PROD/20260312_001/'
);
```

After registration, `SELECT * FROM delta.healthcare.member_summary` works.

**Cross-source query (Delta + PostgreSQL in one SQL):**

```sql
SELECT
    f.file_name,
    f.client_code,
    f.env,
    f.ingestion_status,
    p.records_read,
    p.records_written,
    p.run_status,
    date_diff('second', p.started_at, p.ended_at) AS duration_secs
FROM postgresql.public.file_registry f
JOIN postgresql.public.pipeline_run p
    ON p.pipeline_name = 'claims_curate_job'
ORDER BY f.received_at DESC;
```

This executes entirely in Trino — no application-side join, no data duplication.

**`date_diff` not `EXTRACT(EPOCH)`:**

Trino does not support `EXTRACT(EPOCH FROM interval)`. Use `date_diff('second', start, end)` for duration in seconds. This is a common gotcha when porting SQL from PostgreSQL to Trino.

**Trino CLI:**

```bash
docker exec -it trino trino

# Inside CLI:
SHOW CATALOGS;
SHOW SCHEMAS FROM delta;
SHOW TABLES FROM delta.healthcare;
SELECT * FROM delta.healthcare.member_summary LIMIT 10;
```

---

## Interview Q&A

**Q: What is Trino and how is it different from Spark SQL?**

> Both are distributed SQL engines, but their purpose differs. Spark SQL is a processing engine — it's designed to transform and move data, typically as part of a pipeline job. Trino is a query engine — it's designed for interactive, ad-hoc SQL against data where it lives, with low-latency results. Trino doesn't have a concept of long-running jobs; it's optimized for sub-minute queries. Spark SQL is optimized for large-scale batch transformations. In practice: use Spark SQL to build your Delta tables, use Trino to query them.

**Q: What's the biggest advantage of Trino over loading everything into a warehouse?**

> Federation — a single SQL query can join data from completely different systems without moving or duplicating anything. In DataDissect, the pipeline monitor page runs a query that joins Delta tables in MinIO with audit tables in PostgreSQL. No ETL step, no data duplication, no synchronization lag. The data stays where it's authoritative; Trino just reads it.

**Q: How does Trino handle security and access control?**

> Trino supports multiple auth mechanisms: no-auth (development), password file, LDAP, Kerberos, JWT. Authorization can be delegated to the underlying connector — Trino can use PostgreSQL's own row-level security, or it can be managed centrally with Trino's OPA-based access control or system access control plugins. In production, you'd configure TLS on the coordinator, require authentication, and restrict catalog access per user role.

**Q: What are Trino's limitations?**

> Trino doesn't support transactions or writes to most sources — it's primarily read-only (with some connector exceptions). Queries must fit within the configured memory per node — large sorts or joins that exceed memory fail rather than spilling efficiently to disk (unlike Spark). It's not designed for long-running batch jobs — timeouts and session management are oriented toward interactive use. And it requires all data sources to be accessible at query time — there's no caching or materialization built in (though connectors like Raptor-Legacy did this).

**Q: What's a catalog in Trino?**

> A catalog is a named connector configuration — it tells Trino which connector to use and how to connect to the underlying system. Tables are addressed as `catalog.schema.table`. You can have multiple catalogs of the same connector type — for example, `pipeline_db` pointing at one PostgreSQL database and `reference_db` pointing at another. Catalogs are configured as `.properties` files in `/etc/trino/catalog/`.
