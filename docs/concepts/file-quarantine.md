# File Quarantine & Data Quality Isolation

## What is it

A quarantine pattern separates bad data from good data at ingestion time rather than letting it contaminate downstream systems. It's the data engineering equivalent of a **dead letter queue** in messaging systems — messages (or records) that cannot be processed are routed to a holding zone where they can be inspected, fixed, and reprocessed rather than silently dropped or causing pipeline failures.

Two distinct quarantine scenarios exist in most pipelines:

1. **File-level quarantine** — the entire file is invalid (wrong format, invalid filename convention, corrupted). The file cannot enter the pipeline at all.
2. **Record-level quarantine** — the file is valid but contains individual bad records (missing required fields, invalid values). Good records proceed; bad records are isolated.

Both scenarios share a principle: **nothing is ever silently dropped**. Every piece of data that arrives has a traceable destination — either processed successfully or in quarantine with a reason.

---

## Why we use it here

Healthcare claims have strict validation requirements:
- A file with an invalid filename convention cannot be safely routed — we don't know which client it belongs to, what type it is, or what environment it targets
- Individual claims may have missing required fields (claim_id, member_id, provider_npi) or invalid amounts (billed_amount ≤ 0)

If we rejected these at the pipeline level with an exception, we'd lose the data. If we passed them through, bad records would corrupt aggregations and summaries. Quarantine gives us a third path: preserve everything, process what's valid, isolate what's not, make the isolation visible and auditable.

---

## How it's implemented

**Two quarantine paths:**

| Scenario | Path | Triggered by |
|---|---|---|
| Invalid filename | `s3a://healthcare-quarantine/hold/{timestamp}_{file}` | file_watcher.py before upload |
| Invalid records | `s3a://healthcare-quarantine/claims/{partition}/` | claims_cleanse.py during validation |
| MinIO unreachable | `data/hold/` (local fallback) | file_watcher.py if MinIO upload fails |

**File-level quarantine (`file_watcher.py`):**

```python
def upload_invalid_file(local_path: Path, reason: str) -> None:
    try:
        client = get_minio_client()
        ensure_bucket(client, QUARANTINE_BUCKET)
        timestamp  = datetime.now().strftime("%Y%m%dT%H%M%S")
        object_key = f"hold/{timestamp}_{local_path.name}"
        
        client.fput_object(
            bucket_name=QUARANTINE_BUCKET,
            object_name=object_key,
            file_path=str(local_path),
            content_type="text/csv",
        )
        local_path.unlink()   # delete local after MinIO upload confirms
        
    except Exception as e:
        # MinIO unreachable — fall back to local hold
        HOLD_DIR.mkdir(parents=True, exist_ok=True)
        shutil.move(str(local_path), str(HOLD_DIR / local_path.name))
```

The timestamp prefix ensures uniqueness — two files with the same name arriving at different times don't overwrite each other in quarantine.

The local `data/hold/` fallback is critical: if MinIO is down, the invalid file is still preserved locally rather than being lost. This is a resilience decision — the system degrades gracefully.

**Record-level quarantine (`claims_cleanse.py`):**

```python
validated_df = (
    raw_df
    .withColumn("is_valid",
        col("claim_id").isNotNull()
        & col("member_id").isNotNull()
        & col("provider_npi").isNotNull()
        & col("billed_amount").isNotNull()
        & (col("billed_amount") > 0)
    )
    .withColumn("validation_errors",
        F.concat_ws(", ",
            F.when(col("claim_id").isNull(),      lit("missing claim_id")),
            F.when(col("member_id").isNull(),     lit("missing member_id")),
            F.when(col("provider_npi").isNull(),  lit("missing provider_npi")),
            F.when(col("billed_amount").isNull(), lit("missing billed_amount")),
            F.when(col("billed_amount") <= 0,     lit("invalid billed_amount")),
        )
    )
)

clean_df      = validated_df.filter(col("is_valid")).drop("is_valid", "validation_errors")
quarantine_df = validated_df.filter(~col("is_valid")).drop("is_valid")

# Write quarantine only if there are bad records
if quarantine_count > 0:
    quarantine_df.write.mode("overwrite").parquet(quarantine_path)
```

The `validation_errors` column tells you exactly why each record failed — `"missing claim_id, missing billed_amount"`. This is the information an operations team needs to fix the source data and resubmit.

**Why `concat_ws` for error messages:**

A single record can fail multiple validations simultaneously. `concat_ws(", ", ...)` produces a comma-separated string of all failing rules — `"missing member_id, invalid billed_amount"` — rather than just the first failure. This makes the error actionable.

**MinIO as system of record:**

Once a file is uploaded to MinIO (either raw or quarantine), the local copy is deleted. This is intentional — MinIO is authoritative. Local files are transient landing pads. Keeping local copies after MinIO upload would create two sources of truth.

---

## Interview Q&A

**Q: What happens to a file with the wrong filename format?**

> It's uploaded to `s3a://healthcare-quarantine/hold/{timestamp}_{filename}` with a timestamp prefix to preserve both copies if the same filename arrives multiple times. The local file is deleted after successful upload. If MinIO is unreachable, it falls back to a local `data/hold/` directory — nothing is lost. No DAG is triggered — the file never enters the pipeline.

**Q: What happens to individual bad records within a valid file?**

> During the cleanse stage, every record is evaluated against validation rules — required fields present, billed_amount > 0. Records that fail get a `validation_errors` column listing every failing rule. The clean records are written to `healthcare-cleansed/`, the bad records to `healthcare-quarantine/claims/{partition}/` as a separate Parquet dataset. The pipeline continues with the clean subset — it doesn't fail because of a few bad records.

**Q: How is this different from just failing the pipeline on bad data?**

> Failing the pipeline on any bad record is too brittle — real-world healthcare files always have some messiness. Silently dropping bad records is too dangerous — you'd never know how much data was lost. Quarantine gives you a third option: process what's valid, preserve what's not, make the failure visible and auditable. An operations team can inspect the quarantine, fix the source data, and resubmit just the bad records without reprocessing the whole file.

**Q: How would you operationalize the quarantine — what happens after a file lands there?**

> In production: a monitoring job queries the quarantine bucket daily, counts new records, and fires an alert to the responsible team. The quarantine record includes the original filename and a `validation_errors` column — the ops team sees exactly what's wrong. They contact the client to fix the source data, resubmit corrected records, and the fixed file gets a new sequence number (`_002`) to distinguish it from the original `_001`. The original quarantined data is retained for the audit period (typically 7 years for healthcare).
