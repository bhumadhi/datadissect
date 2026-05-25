# Medallion Architecture

## What is it

Medallion architecture (also called Bronze/Silver/Gold or Raw/Cleansed/Curated) is a layered data lake pattern where data moves through progressively refined zones. Each zone has a specific contract:

- **Raw / Bronze** — exact copy of source data, immutable, never modified. System of record. If something goes wrong, you re-derive everything downstream from here.
- **Cleansed / Silver** — validated, standardized, deduplicated. Bad records separated out. PHI masked. Schema enforced. Still row-level, still close to source shape.
- **Transformed / Gold (stage 1)** — enriched with reference data, derived fields added, business logic applied. Still row-level but now analytics-ready.
- **Curated / Gold (stage 2)** — aggregated summaries built for specific consumers (member view, payer view, provider view). Optimized for reads, not joins.

The key principle: **you never modify upstream zones**. Each layer re-derives from the one above it. This gives you full reprocessability — if business logic changes, rerun from cleansed. If validation rules change, rerun from raw.

---

## Why we use it here

Claims data arrives raw from clients via SFTP. It contains PHI, formatting inconsistencies, and occasionally bad records. We need to:

1. Preserve exactly what the client sent (audit requirement, dispute resolution)
2. Produce clean, masked, analytics-safe data for downstream
3. Build aggregated views that Trino can query efficiently

The zoned approach means we can fix a bug in the transform job and rerun it without touching raw. We can change the curated aggregation without reprocessing cleansed data. Each zone is independently reprocessable.

---

## How it's implemented

**Zone → Storage format → Location**

| Zone | Format | MinIO bucket |
|---|---|---|
| Raw | CSV (original) | `healthcare-raw/{partition}/{filename}` |
| Cleansed | Parquet | `healthcare-cleansed/claims/{partition}/` |
| Transformed | Delta Lake | `healthcare-transformed/claims/{partition}/` |
| Curated | Delta Lake | `healthcare-curated/{summary_type}/{partition}/` |

**Why Parquet at cleansed, Delta at transform?**

Cleansed is ephemeral staging. If we need to reprocess, we re-cleanse from raw — so version history adds no value. Parquet is lighter, faster to write, no transaction log overhead.

Delta starts at transform because that's where data has **business value worth protecting**:
- ACID: if the transform job crashes halfway, we don't land a partial dataset
- Time travel: if business logic changes, we can see what the data looked like before
- Schema enforcement: downstream consumers can rely on a contract

This is a deliberate tradeoff — not "Delta everywhere" (overkill) or "Parquet everywhere" (no protection where it matters).

**Path utility — single source of truth:**

All zone paths are defined in `processing/common/path_utils.py`:

```python
def raw_input_path(meta: FileMeta) -> str:
    return f"s3a://healthcare-raw/{meta.partition}/{meta.file_name}"

def cleansed_output_path(meta: FileMeta) -> str:
    return f"s3a://healthcare-cleansed/claims/{meta.partition}/"

def transformed_output_path(meta: FileMeta) -> str:
    return f"s3a://healthcare-transformed/claims/{meta.partition}/"

def curated_member_summary_path(meta: FileMeta) -> str:
    return f"s3a://healthcare-curated/member_summary/{meta.partition}/"
```

No hardcoded paths anywhere else in the codebase. Change the zone structure once, it propagates everywhere.

**Airflow validates between every zone:**

```
check_file_exists → cleanse → validate_cleansed → transform → validate_transformed → curate → validate_curated
```

Each validate task checks that parquet files exist at the expected prefix before allowing the next stage to start. If cleansed output is missing, the pipeline fails before transform runs.

---

## Interview Q&A

**Q: Walk me through your data zones.**

> Raw is the immutable system of record — exact copy of what the client sent. Cleansed is validated, PHI-masked, bad records quarantined — still row-level but analytics-safe. Transformed adds enrichment from reference data and derived fields. Curated is pre-aggregated summaries per business entity: member, payer, provider. Each zone is independently reprocessable from the one above.

**Q: Why Parquet at cleansed but Delta at transform?**

> Cleansed is ephemeral — if we need to reprocess we re-cleanse from raw, so version history adds no value. Delta starts at transform where data has real business value: ACID protects against partial writes, time travel lets us audit how data looked before a logic change, schema enforcement gives downstream consumers a contract they can rely on.

**Q: What's the benefit of immutable raw?**

> You get full reprocessability and an audit trail. If a client disputes a claim outcome, you can show them exactly what they sent. If your validation rules change, you re-derive from raw — nothing is lost. Raw is append-only by convention: new files land in new partition paths, existing objects are never overwritten.

**Q: How is this different from a traditional data warehouse ETL?**

> Traditional ETL often transforms in place — you can't see what the data looked like before the transformation. Medallion keeps every stage as a separate dataset. You trade storage space for full lineage and reprocessability. In a cloud object store where storage is cheap, that's almost always the right tradeoff.
