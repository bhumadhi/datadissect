# Delta Lake

## What is it

Delta Lake is an open-source storage layer that sits on top of Parquet files and adds:

1. **ACID transactions** — writes are atomic. Either the full write commits or nothing does. No partial datasets.
2. **Transaction log (`_delta_log/`)** — a JSON log of every operation (add file, remove file, schema change). This is how all other features are built.
3. **Time travel** — because every version is recorded in the log, you can query any previous snapshot: `df.read.format("delta").option("versionAsOf", 3).load(path)`
4. **Schema enforcement** — writes that don't match the table schema are rejected at write time, not discovered later by a broken query.
5. **Schema evolution** — when you legitimately need to add a column: `.option("mergeSchema", "true")`
6. **Upserts (MERGE)** — `MERGE INTO target USING source ON condition WHEN MATCHED THEN UPDATE WHEN NOT MATCHED THEN INSERT` — essential for CDC patterns.

Delta files on disk look like:

```
healthcare-transformed/claims/BCBS001/837P/PROD/20260312_001/
  _delta_log/
    00000000000000000000.json    ← commit 0 (CREATE TABLE)
    00000000000000000001.json    ← commit 1 (first write)
  part-00000-xxxx.snappy.parquet
  part-00001-xxxx.snappy.parquet
```

The `_delta_log/` is what makes it Delta — without it, it's just Parquet.

---

## Why we use it here

At the transform and curate stages, data has real business value. Two failure scenarios we need to protect against:

1. **Partial write crash** — Spark job writes 3 of 5 partitions then dies. Without Delta, you get a corrupt dataset. With Delta, the transaction never commits — the table is still at the previous clean state.
2. **Logic change reprocessing** — business changes the billed_category thresholds. Without time travel, you can't see what the data looked like before. With Delta, `versionAsOf` gives you the old snapshot for comparison.

We deliberately don't use Delta at the cleansed layer because that's ephemeral staging — if something goes wrong we re-cleanse from raw. The transaction log overhead isn't worth it there.

---

## How it's implemented

**SparkSession configuration for Delta:**

Transform and curate jobs use `configure_spark_with_delta_pip` instead of `SparkSession.builder` directly:

```python
from delta import configure_spark_with_delta_pip

builder = (
    SparkSession.builder
    .appName("Claims-Transform-Job")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    # ... S3A MinIO configs
)
spark = configure_spark_with_delta_pip(builder).getOrCreate()
```

The two `spark.sql.*` configs register the Delta catalog and SQL extensions. Without them, Spark doesn't know how to write Delta format.

**spark-submit package for Delta:**

```bash
spark-submit \
  --packages io.delta:delta-spark_2.12:3.0.0,\
             org.apache.hadoop:hadoop-aws:3.3.4,\
             com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  claims_transform.py
```

`delta-spark_2.12:3.0.0` — the `_2.12` is the Scala version that Spark 3.5.1 is compiled against. Must match exactly. Delta 3.0.0 pairs with Spark 3.5.x.

**Writing Delta:**

```python
transformed_df.write.format("delta").mode("overwrite").save(transformed_output_path(meta))
```

`mode("overwrite")` on Delta is safe — it's a new transaction that atomically replaces the previous version. The old version is still in the transaction log (time travel still works).

**Why version pinning matters:**

Delta Lake version must align with Spark version:
- Delta 3.0.0 → Spark 3.5.x
- Delta 2.4.0 → Spark 3.4.x

Mixing versions causes `ClassNotFoundException` or silent data corruption. Airflow's default pip resolution was pulling PySpark 4.x which broke Delta 3.0.0 — pinning `pyspark==3.5.1` in the Dockerfile fixed this.

---

## Interview Q&A

**Q: What is Delta Lake and why would you use it over plain Parquet?**

> Delta Lake is a storage layer on top of Parquet that adds a transaction log. That log gives you ACID transactions — writes are atomic, so a crashed job doesn't leave a partial dataset. It gives you time travel — you can query any previous version. And schema enforcement — writes that don't match the table schema are rejected at write time. I use plain Parquet where data is ephemeral and I'd re-derive it anyway, and Delta where data has business value worth protecting.

**Q: How does ACID work in a distributed system like Spark?**

> Delta's transaction log is a series of JSON files in `_delta_log/`. A write creates a new JSON entry listing which Parquet files were added or removed. That log entry is written atomically using the underlying filesystem's atomic rename or put-if-absent semantics. Readers always check the log first to get the current snapshot, so they never see a partially written dataset. The log is also how Delta handles concurrent writers — it detects conflicts using optimistic concurrency control.

**Q: What's time travel and when would you actually use it?**

> Time travel lets you query Delta tables at a specific version or timestamp: `df.read.format("delta").option("versionAsOf", 5).load(path)`. Real use cases: auditing — "what did this table look like last Tuesday before that job ran?"; debugging — comparing before and after a logic change; rollback — if a bad write commits, you can read the previous version and overwrite. In healthcare especially, being able to show exactly what data looked like at a specific point has compliance value.

**Q: What's the difference between Delta Lake, Apache Iceberg, and Apache Hudi?**

> All three are open table formats that add a transaction log on top of Parquet. Delta is tightest with Spark and Databricks — native Spark integration, simplest to set up. Iceberg is more engine-agnostic — works equally well with Flink, Trino, Spark. Hudi is optimized for streaming upserts and CDC patterns — it has built-in record-level upsert support that Delta achieves via MERGE. For a Spark-first shop, Delta is the natural choice. For multi-engine environments, Iceberg. For high-frequency CDC ingestion, Hudi.
