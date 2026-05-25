# Apache Spark + MinIO (S3-Compatible Object Storage)

## What is it

**Apache Spark** is a distributed in-memory data processing engine. It reads data, applies transformations lazily (builds a DAG of operations), then executes when an action is called (`count()`, `write`, `show()`). Key properties:
- **Lazy evaluation** — transformations don't execute until an action triggers them. Spark can optimize the full plan before running.
- **DAG execution** — Spark builds a Directed Acyclic Graph of stages. Each stage is a set of tasks that can run in parallel across partitions.
- **In-memory** — intermediate results are kept in memory where possible. Falls back to disk when memory is exhausted.
- **Local mode** — `SparkSession.builder.master("local[*]")` runs all tasks on the current machine using all CPU cores. Same code runs in local mode or on a cluster without changes.

**MinIO** is an S3-compatible object store — it implements the Amazon S3 API exactly. Any tool that talks to S3 talks to MinIO identically. This means Spark's S3A connector works with MinIO without modification.

**S3A** is Hadoop's filesystem connector for S3-compatible storage. It translates Spark's file I/O calls into HTTP requests against the S3 API. MinIO → S3A → Spark is the full chain.

---

## Why we use it here

MinIO replaces HDFS as the distributed storage layer. HDFS requires a full Hadoop cluster — complex to run locally. MinIO runs as a single Docker container, exposes the S3 API, and stores data on local disk. For local development on a Mac Mini, this is the right tradeoff — same API as production S3, zero cluster management.

Spark runs in `local[*]` mode — uses all cores on the Mac Mini (M4 has 10 cores), no cluster needed. Same PySpark code would run unchanged on a real Spark cluster against real S3.

---

## How it's implemented

**SparkSession configuration (cleanse job):**

```python
spark = (
    SparkSession.builder
    .appName("Claims-Cleanse-Job")
    .config("spark.hadoop.fs.s3a.endpoint",               "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key",             "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key",             "minioadmin123")
    .config("spark.hadoop.fs.s3a.path.style.access",      "true")
    .config("spark.hadoop.fs.s3a.impl",                   "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.connection.timeout",     "600000")
    .config("spark.hadoop.fs.s3a.socket.timeout",         "600000")
    .getOrCreate()
)
```

**Config breakdown:**

| Config | Value | Why |
|---|---|---|
| `fs.s3a.endpoint` | `http://minio:9000` | MinIO API address. Container name `minio` resolves inside Docker network. `localhost` would fail from inside Airflow container. |
| `fs.s3a.path.style.access` | `true` | S3 standard uses virtual-hosted style: `bucket.endpoint/key`. MinIO requires path style: `endpoint/bucket/key`. Without this, every request fails with 400. |
| `fs.s3a.impl` | `S3AFileSystem` | Tells Hadoop which filesystem class to use for `s3a://` URLs. |
| `fs.s3a.connection.ssl.enabled` | `false` | MinIO is running HTTP, not HTTPS locally. |
| `connection.timeout` / `socket.timeout` | `600000` (ms) | Large Spark jobs can stall on slow MinIO writes. `"60s"` string format is rejected — must be numeric milliseconds. |

**spark-submit packages:**

Cleanse job (no Delta):
```bash
spark-submit \
  --master local[*] \
  --packages org.apache.hadoop:hadoop-aws:3.3.4,\
             com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  claims_cleanse.py --file-name BCBS001_837P_PROD_20260312_001.csv
```

Transform/Curate jobs (with Delta):
```bash
spark-submit \
  --master local[*] \
  --packages io.delta:delta-spark_2.12:3.0.0,\
             org.apache.hadoop:hadoop-aws:3.3.4,\
             com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  claims_transform.py --file-name BCBS001_837P_PROD_20260312_001.csv
```

**Package version alignment:**

| Package | Version | Why pinned |
|---|---|---|
| `hadoop-aws` | 3.3.4 | Must match the Hadoop version bundled with Spark 3.5.1. Mixing versions causes `NoSuchMethodError` at runtime. |
| `aws-java-sdk-bundle` | 1.12.262 | The AWS SDK that `hadoop-aws` calls. Must be a version compatible with `hadoop-aws:3.3.4`. |
| `delta-spark_2.12` | 3.0.0 | `_2.12` = Scala 2.12 (Spark 3.5.1 binary). Delta 3.0.0 pairs with Spark 3.5.x. |

**Reading and writing:**

```python
# Read CSV from MinIO
raw_df = spark.read.option("header", "true").option("inferSchema", "true").csv(input_path)

# Write Parquet
cleansed_df.write.mode("overwrite").parquet(clean_path)

# Write Delta
transformed_df.write.format("delta").mode("overwrite").save(transformed_path)
```

Paths use `s3a://` scheme: `s3a://healthcare-raw/BCBS001/837P/PROD/20260312_001/file.csv`

**`count()` optimization:**

```python
raw_count = raw_df.count()        # materialize once, store in variable
# ... transformations ...
cleansed_count = cleansed_df.count()   # separate count after filter
```

Each `count()` triggers a full Spark job. Calling it multiple times on the same DataFrame re-executes the full plan each time. Store counts in variables. Use `cache()` when you need to scan the same DataFrame multiple times for different purposes.

---

## Interview Q&A

**Q: How does Spark connect to MinIO?**

> MinIO implements the S3 API, so Spark's S3A connector works with it directly. The key configs are: endpoint pointing at the MinIO container, `path.style.access=true` (MinIO uses path-style URLs, not virtual-hosted), and SSL disabled for local HTTP. Inside Docker, services communicate by container name — `minio:9000` not `localhost:9000`.

**Q: What's the difference between `local[*]` and a real cluster?**

> `local[*]` runs all Spark tasks on the current machine using all available CPU cores. The same PySpark code runs unchanged on a real cluster — you'd change `--master` to point at your cluster manager (YARN, standalone, Kubernetes) and add worker configuration. Local mode is valid for development and moderate data volumes; you lose fault tolerance and horizontal scale.

**Q: Why can't you use `localhost` inside the Airflow container to reach MinIO?**

> Each Docker container has its own network namespace. `localhost` inside the Airflow container refers to the Airflow container itself, not the host machine or other containers. Services on the same Docker network communicate by container name — `minio` resolves to the MinIO container's internal IP. This is why the `.env` file has `MINIO_ENDPOINT=http://minio:9000` but `file_watcher.py` (which runs on the host) uses `localhost:9000`.

**Q: What's lazy evaluation and why does it matter?**

> Spark transformations like `filter()`, `withColumn()`, `join()` don't execute immediately — they build a logical plan. Execution only happens when an action like `count()`, `write`, or `show()` is called. At that point, Spark's Catalyst optimizer analyzes the full plan and may reorder operations, push filters down to reduce data read, or merge stages. Lazy evaluation means you can chain many transformations efficiently — the optimizer sees the full picture before deciding how to execute.

**Q: Why is `inferSchema=true` on CSV reads something to be careful about?**

> `inferSchema` requires Spark to read the entire file twice — once to sample and infer types, once to actually read data. On large files this is expensive. It can also infer wrong types (a column of "001" strings gets inferred as integers, losing the leading zero). In production, define the schema explicitly with `StructType` — faster and deterministic.
