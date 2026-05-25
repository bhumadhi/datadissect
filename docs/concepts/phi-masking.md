# PHI Masking & Data Privacy

## What is it

**PHI (Protected Health Information)** is any information that can identify a patient in a healthcare context — name, date of birth, SSN, member ID, provider NPI, diagnosis codes linked to a person, etc. In the US, PHI is regulated by **HIPAA (Health Insurance Portability and Accountability Act)**.

**De-identification** is the process of removing or transforming PHI so the data can no longer be linked to an individual. HIPAA defines two accepted methods:

1. **Safe Harbor** — remove 18 specific identifiers (name, ZIP, dates more granular than year, phone, SSN, MRN, etc.). Dates must be generalized to year only for patients over 89.
2. **Expert Determination** — a statistical expert certifies the re-identification risk is very low. Allows more flexible transformation.

**Masking techniques:**

| Technique | Reversible? | Joinable? | Use case |
|---|---|---|---|
| **Deletion** | No | No | Fields you never need downstream |
| **Generalization** | No | Partial | Dates → year only, ZIP → 3-digit |
| **Encryption (AES)** | Yes (with key) | Yes (same key) | When you need to decrypt for authorized users |
| **Tokenization** | Yes (with lookup table) | Yes | Payment card data, secure vault |
| **Hashing (SHA-256)** | No | Yes (same input = same hash) | Analytics that need cross-file joins without exposing raw values |
| **Pseudonymization** | Yes (with mapping) | Yes | GDPR-compliant re-identification path preserved |

---

## Why we use it here

Claims processing requires analyzing patterns across members and providers — which members have chronic conditions, which providers have high claim volumes. But we must not expose raw `member_id` or `provider_npi` in the analytics layer.

**SHA-256 hashing** is the right tool here because:
1. **One-way** — cannot reverse the hash to get the original value
2. **Deterministic** — same `member_id` always produces the same hash. A member who appears in the March file AND the April file will have the same `member_id_hash` in both — cross-file joins still work without exposing the real ID
3. **No key management** — unlike encryption, hashing doesn't require a key store or key rotation
4. **Fast** — SHA-256 is a CPU-native operation, negligible overhead at Spark scale

---

## How it's implemented

**In `claims_cleanse.py`:**

```python
from pyspark.sql.functions import sha2, col

cleansed_df = (
    clean_df
    .withColumn("member_id_hash",    sha2(col("member_id").cast("string"),    256))
    .withColumn("provider_npi_hash", sha2(col("provider_npi").cast("string"), 256))
    .drop("member_id", "provider_npi")   # originals removed — never written downstream
)
```

**What happens step by step:**
1. `sha2(col("member_id"), 256)` — Spark applies SHA-256 to each value in the column
2. Result stored as a new column `member_id_hash` (64-character hex string)
3. Original `member_id` column dropped — it does not appear in cleansed, transformed, or curated output
4. Same for `provider_npi` → `provider_npi_hash`

**Why `.cast("string")` before hashing:**

SHA-256 operates on bytes/strings. If `member_id` was inferred as an integer by Spark's CSV reader, casting ensures consistent hash input format — `"12345"` not integer `12345`.

**Cross-file join still works:**

```
File March: member_id = "M10042" → hash = "a3f7c2..."
File April: member_id = "M10042" → hash = "a3f7c2..."
```

The same member appears across months with the same hash — you can aggregate their claims without ever exposing `M10042`.

**Production addition — secret salt:**

```python
SALT = os.getenv("PHI_HASH_SALT")   # stored in AWS KMS / Vault
sha2(concat(col("member_id"), lit(SALT)), 256)
```

Without a salt, SHA-256 is vulnerable to **rainbow table attacks** — a precomputed table of `hash(common_value) → value`. An attacker who knows that member IDs follow a pattern (e.g., `M` + 5 digits) can precompute all 99,999 hashes and reverse-look up any hash they see. A secret salt prevents this — the attacker would need the salt to build the table. In local development the salt is omitted for simplicity; in production it must be in a secrets manager.

---

## Interview Q&A

**Q: How do you handle PHI in your pipeline?**

> PHI is masked at the cleanse stage — the earliest possible point after validation. Member ID and provider NPI are SHA-256 hashed into new columns, and the originals are dropped. Raw data in `healthcare-raw/` retains original PHI and is access-controlled. Everything from cleansed onward contains only hashes — downstream jobs, Trino queries, and Streamlit dashboards never see real PHI.

**Q: Why SHA-256 hashing instead of encryption?**

> Encryption is reversible — you need key management, key rotation, and secure key storage. For analytics, we don't need to reverse the value — we need stable identifiers for joins and aggregations. SHA-256 gives us that: same input always produces the same hash, so we can join a member's March claims to their April claims using the hash. One-way means no decryption risk, no key management overhead.

**Q: What's a rainbow table attack and how do you prevent it?**

> A rainbow table is a precomputed mapping of hash(value) → value for common inputs. If member IDs follow a predictable pattern, an attacker could compute all possible hashes and reverse-look up any hash they see in the data. The defense is a secret salt — append a random secret string to the value before hashing: `sha256(member_id + salt)`. Without the salt, the rainbow table is useless. The salt lives in a secrets manager (AWS KMS, HashiCorp Vault), never in code or environment files.

**Q: What's the difference between de-identification and pseudonymization?**

> De-identification removes or transforms PHI so the data cannot be linked back to an individual — it's intended to be irreversible. Pseudonymization replaces identifiers with pseudonyms (tokens, hashes) while preserving a mapping that allows re-identification by authorized parties. Under GDPR, pseudonymized data is still considered personal data because re-identification is possible. Under HIPAA Safe Harbor, properly de-identified data is no longer PHI. Our SHA-256 approach without a lookup table is closer to de-identification — you can't reverse it. With a lookup table it would be pseudonymization.

**Q: What is HIPAA and what does it require for a data pipeline?**

> HIPAA is the US federal law governing protection of patient health information. For a data pipeline, the key obligations are: encrypt PHI in transit (TLS) and at rest, enforce access controls (who can read raw vs cleansed data), maintain audit logs of who accessed what and when, and ensure PHI is not retained longer than necessary. De-identification is one strategy to reduce the scope of HIPAA obligations — data that has been properly de-identified is no longer PHI and falls outside HIPAA's scope.
