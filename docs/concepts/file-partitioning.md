# File Partitioning & Path Design

## What is it

Partitioning in object storage means organizing files into a directory hierarchy that mirrors the most common query dimensions. When Spark reads a partitioned dataset, it can skip entire directory subtrees based on the query filter — this is called **predicate pushdown** or **partition pruning**.

**How it works:**

When you read `s3a://bucket/claims/BCBS001/837P/PROD/*/` Spark lists only that prefix — it never touches `BCBS002/`, `837I/`, or `TEST/`. On datasets with millions of files, scanning the right prefix vs the entire bucket can be the difference between a 10-second query and a 10-minute query.

**Hive-style partitioning** (used by Spark by default) encodes partition values in directory names:
```
year=2026/month=03/day=12/part-00000.parquet
```

Spark reads these and automatically adds `year`, `month`, `day` as columns without them being in the Parquet file itself.

**Key design decisions for a partition scheme:**
1. **Cardinality** — high-cardinality columns (e.g., claim_id, member_id) make terrible partition keys — too many tiny directories. Low-to-medium cardinality is ideal.
2. **Query patterns** — partition by what you filter on most often.
3. **Write pattern** — all data for one file lands in one partition path, avoiding the small-files problem.

---

## Why we use it here

Claims data is queried by client, file type, environment, and date. Reading all BCBS001 professional claims across multiple days should not require scanning Aetna or institutional claim data. Partitioning by `CLIENT/FILE_TYPE/ENV/DATE_SEQ` matches the natural query access pattern.

Additionally: each file is one unit of work. When Spark writes one file's cleansed output, it writes to one partition path — no merging across files required, no file-boundary coordination.

---

## How it's implemented

**The partition key — `FileMeta.partition`:**

```python
@dataclass(frozen=True)
class FileMeta:
    client_code: str   # BCBS001
    file_type:   str   # 837P
    env:         str   # PROD
    date:        str   # 20260312
    sequence:    str   # 001

    @property
    def partition(self) -> str:
        return f"{self.client_code}/{self.file_type}/{self.env}/{self.date}_{self.sequence}"
        # → BCBS001/837P/PROD/20260312_001
```

Every path function uses this property:

```python
def cleansed_output_path(meta: FileMeta) -> str:
    return f"s3a://healthcare-cleansed/claims/{meta.partition}/"
    # → s3a://healthcare-cleansed/claims/BCBS001/837P/PROD/20260312_001/
```

**The full MinIO zone map:**

```
healthcare-raw/
  BCBS001/837P/PROD/20260312_001/BCBS001_837P_PROD_20260312_001.csv

healthcare-cleansed/
  claims/BCBS001/837P/PROD/20260312_001/
    part-00000-xxxx.parquet

healthcare-transformed/
  claims/BCBS001/837P/PROD/20260312_001/          ← row-level Delta
    _delta_log/
    part-00000-xxxx.parquet
  summaries/payer/BCBS001/837P/PROD/20260312_001/ ← payer summary Delta
  summaries/cpt/BCBS001/837P/PROD/20260312_001/   ← CPT summary Delta

healthcare-curated/
  member_summary/BCBS001/837P/PROD/20260312_001/
  payer_summary/BCBS001/837P/PROD/20260312_001/
  provider_summary/BCBS001/837P/PROD/20260312_001/

healthcare-quarantine/
  claims/BCBS001/837P/PROD/20260312_001/   ← rejected records from cleanse
  hold/{timestamp}_{filename}              ← invalid filename convention

healthcare-reference/
  cpt_codes.csv
  icd10_codes.csv

healthcare-scratch/
  {username}/{label}/                      ← 7-day TTL lifecycle policy
```

**Why `DATE_SEQ` combined as the leaf (`20260312_001` not `20260312/001`):**

Date and sequence are almost always queried together — you look up "the 001 file from March 12th" as a unit. Keeping them at the same directory level means a glob like `20260312_*/` matches all sequences for that date, and `20260312_001/` pinpoints an exact file. Splitting them into separate levels (`20260312/001/`) would require deeper path traversal for a common access pattern.

**Why `ENV` is in the path (not just in the filename):**

Clients send PROD, TEST, UAT data via the same SFTP channel — they can't control folder structure. ENV in the filename convention makes each file self-describing. The partition path then separates them so a Spark job reading `ENV=PROD` doesn't accidentally process `ENV=TEST` data. Critical for compliance — PROD data must not mix with test data.

**Airflow validation uses file-specific prefix:**

```python
def check_parquet_files(client: Minio, bucket: str, prefix: str) -> int:
    objects = list(client.list_objects(bucket, prefix=prefix, recursive=True))
    parquet_files = [o for o in objects if o.object_name.endswith(".parquet")]
    if not parquet_files:
        raise Exception(f"No parquet files found in {bucket}/{prefix}")
    return len(parquet_files)
```

The prefix is always the full partition path for this specific file — `claims/BCBS001/837P/PROD/20260312_001/`. This means validation fails if *this file's* output is missing, even if other files' output exists in the bucket.

---

## Interview Q&A

**Q: How did you design your partition scheme and why?**

> The partition key is `CLIENT/FILE_TYPE/ENV/DATE_SEQ`. I chose these dimensions because they match the natural query access pattern — reading all professional claims for BCBS001 across multiple dates scans only `BCBS001/837P/PROD/*/`, not the entire bucket. Each file produces one partition path, which avoids small-files problems and makes per-file validation straightforward.

**Q: What's predicate pushdown / partition pruning?**

> When Spark reads a partitioned dataset with a filter like `WHERE client_code = 'BCBS001'`, it translates that filter into a path prefix scan — it only lists and reads files under the `BCBS001/` directory. Directories for other clients are never touched. This is purely a directory listing optimization — no data is read from skipped partitions, not even metadata.

**Q: What's the small files problem and how does partitioning affect it?**

> Object stores and HDFS perform poorly with millions of tiny files — each file requires a separate metadata operation to read. Over-partitioning (partitioning by a high-cardinality column like claim_id) creates one tiny file per value. Our partition scheme is designed so that one file drop = one partition path = one set of Parquet part files from the Spark write. The number of part files depends on Spark's parallelism (number of partitions in the DataFrame), typically a few files per write — manageable.

**Q: Why is ENV in both the filename and the path?**

> Filename convention: clients send via SFTP and can't control folder structure, so the file must be self-describing. Path convention: separating by ENV in the path ensures a Spark read of `ENV=PROD` data cannot accidentally include `ENV=TEST` data. Compliance requirement — PROD and non-PROD data must be physically separated, not just logically filtered.
