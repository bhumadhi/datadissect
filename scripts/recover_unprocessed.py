from __future__ import annotations

"""
recover_unprocessed.py — scans data/inbound/ for files that arrived
but were never fully processed (watcher was down, crash, etc.).

Compares inbound files against file_registry and re-triggers the
Airflow DAG for any file not in CURATED status.

Valid files:
  → uploaded to s3a://healthcare-raw/{partition}/
  → deleted from data/inbound/ after successful upload
  → DAG triggered

Invalid files:
  → uploaded to s3a://healthcare-quarantine/hold/{timestamp}_{filename}
  → deleted from data/inbound/
  → no DAG trigger

Usage:
    # Dry run — show what would happen, no changes
    python scripts/recover_unprocessed.py --dry-run

    # Live run — per-file confirmation
    python scripts/recover_unprocessed.py

    # Specific file only
    python scripts/recover_unprocessed.py --file-name BCBS001_837P_PROD_20260312_001.csv

    # Force re-trigger even if already CURATED
    python scripts/recover_unprocessed.py --force
"""

import argparse
import logging
import os
import shutil
import sys
from datetime import datetime
from pathlib import Path

import psycopg2
import requests
from minio import Minio

# ── Path Utils ───────────────────────────────────────────────
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "processing"))
from common.path_utils import FileMeta, parse_filename

# ── Logging ──────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger(__name__)

# ── Config ───────────────────────────────────────────────────
PROJECT_ROOT      = Path(__file__).resolve().parent.parent
INBOUND_DIR       = PROJECT_ROOT / "data" / "inbound"
HOLD_DIR          = PROJECT_ROOT / "data" / "hold"   # fallback only

RAW_BUCKET        = "healthcare-raw"
QUARANTINE_BUCKET = "healthcare-quarantine"

MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT",   "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin123")

AIRFLOW_BASE_URL = os.getenv("AIRFLOW_BASE_URL",      "http://localhost:8082")
AIRFLOW_USER     = os.getenv("AIRFLOW_ADMIN_USER",     "admin")
AIRFLOW_PASSWORD = os.getenv("AIRFLOW_ADMIN_PASSWORD", "admin123")
DAG_ID           = "claims_pipeline"

POSTGRES_HOST     = os.getenv("POSTGRES_HOST",     "localhost")
POSTGRES_PORT     = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB       = os.getenv("POSTGRES_DB",       "pipeline_db")
POSTGRES_USER     = os.getenv("POSTGRES_USER",     "pgadmin")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "pgpassword123")

# Terminal colors
GREEN  = "\033[92m"
YELLOW = "\033[93m"
RED    = "\033[91m"
CYAN   = "\033[96m"
RESET  = "\033[0m"
BOLD   = "\033[1m"


# ── Helpers ──────────────────────────────────────────────────
def get_minio_client() -> Minio:
    endpoint = MINIO_ENDPOINT.replace("http://", "").replace("https://", "")
    return Minio(endpoint, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=False)


def get_postgres_connection():
    return psycopg2.connect(
        host=POSTGRES_HOST, port=POSTGRES_PORT,
        database=POSTGRES_DB, user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )


def ensure_bucket(client: Minio, bucket: str) -> None:
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)
        logger.info("Created bucket: %s", bucket)


def get_registry_status(file_name: str) -> str | None:
    """Returns most recent ingestion_status, or None if never registered."""
    conn = None
    cursor = None
    try:
        conn = get_postgres_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT ingestion_status FROM file_registry
            WHERE file_name = %s
            ORDER BY received_at DESC LIMIT 1
            """,
            (file_name,),
        )
        row = cursor.fetchone()
        return row[0] if row else None
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def upload_valid_file(meta: FileMeta, local_path: Path) -> bool:
    """
    Upload valid file to MinIO raw bucket.
    Skips if already present. Deletes local after upload.
    Returns True if uploaded, False if already existed.
    """
    client = get_minio_client()
    ensure_bucket(client, RAW_BUCKET)
    object_key = f"{meta.partition}/{meta.file_name}"

    # Check if already in MinIO
    try:
        client.stat_object(RAW_BUCKET, object_key)
        logger.info("  Already in MinIO: s3a://%s/%s", RAW_BUCKET, object_key)
        # Still delete local — MinIO is the system of record
        local_path.unlink()
        logger.info("  Deleted local duplicate: %s", local_path.name)
        return False
    except Exception:
        pass

    client.fput_object(
        bucket_name=RAW_BUCKET,
        object_name=object_key,
        file_path=str(local_path),
        content_type="text/csv",
    )
    logger.info("  Uploaded → s3a://%s/%s ✅", RAW_BUCKET, object_key)

    local_path.unlink()
    logger.info("  Deleted local file after upload: %s", local_path.name)
    return True


def upload_invalid_file(local_path: Path, reason: str) -> None:
    """
    Upload invalid file to MinIO quarantine/hold/.
    Deletes local after upload.
    Fallback: move to data/hold/ if MinIO unreachable.
    """
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
        logger.warning(
            "  Invalid → s3a://%s/%s | Reason: %s",
            QUARANTINE_BUCKET, object_key, reason,
        )
        local_path.unlink()
        logger.info("  Deleted local invalid file: %s", local_path.name)

    except Exception as e:
        logger.error("  MinIO quarantine failed (%s) — falling back to data/hold/", e)
        HOLD_DIR.mkdir(parents=True, exist_ok=True)
        dest = HOLD_DIR / local_path.name
        shutil.move(str(local_path), str(dest))
        logger.warning("  Fallback: moved to %s", dest)


def trigger_dag(meta: FileMeta) -> str:
    """Trigger Airflow DAG. Returns dag_run_id."""
    url = f"{AIRFLOW_BASE_URL}/api/v1/dags/{DAG_ID}/dagRuns"
    response = requests.post(
        url,
        json={"conf": {"file_name": meta.file_name}},
        auth=(AIRFLOW_USER, AIRFLOW_PASSWORD),
        timeout=30,
    )
    response.raise_for_status()
    return response.json().get("dag_run_id", "unknown")


def scan_inbound() -> list[Path]:
    if not INBOUND_DIR.exists():
        logger.warning("Inbound directory does not exist: %s", INBOUND_DIR)
        return []
    return sorted(INBOUND_DIR.glob("*.csv"))


# ── Args ─────────────────────────────────────────────────────
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Recover unprocessed files from data/inbound/",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scripts/recover_unprocessed.py --dry-run
  python scripts/recover_unprocessed.py
  python scripts/recover_unprocessed.py --file-name BCBS001_837P_PROD_20260312_001.csv
  python scripts/recover_unprocessed.py --force
        """
    )
    parser.add_argument("--dry-run",   action="store_true", help="Show what would happen, no changes")
    parser.add_argument("--file-name", help="Process a specific file only")
    parser.add_argument("--force",     action="store_true", help="Re-trigger even if already CURATED")
    return parser.parse_args()


# ── Main ─────────────────────────────────────────────────────
def main() -> None:
    args = parse_args()

    print(f"\n{BOLD}{'='*62}{RESET}")
    print(f"{BOLD}  Healthcare Pipeline — Recovery Tool{RESET}")
    print(f"  Inbound:    {INBOUND_DIR}")
    print(f"  Raw bucket: s3a://{RAW_BUCKET}/")
    print(f"  Quarantine: s3a://{QUARANTINE_BUCKET}/hold/")
    if args.dry_run:
        print(f"  {YELLOW}Mode: DRY RUN — no changes will be made{RESET}")
    elif args.force:
        print(f"  {YELLOW}Mode: FORCE — will re-trigger even CURATED files{RESET}")
    else:
        print(f"  Mode: LIVE — per-file confirmation")
    print(f"{BOLD}{'='*62}{RESET}\n")

    # Get files to process
    if args.file_name:
        specific = INBOUND_DIR / args.file_name
        if not specific.exists():
            print(f"{RED}File not found in inbound: {args.file_name}{RESET}")
            sys.exit(1)
        files = [specific]
    else:
        files = scan_inbound()

    if not files:
        print(f"{YELLOW}No CSV files found in {INBOUND_DIR}{RESET}\n")
        return

    print(f"Found {BOLD}{len(files)}{RESET} CSV file(s):\n")

    # ── Categorize ────────────────────────────────────────────
    to_trigger = []   # (path, meta, status)
    to_skip    = []   # (path, meta, status)
    invalid    = []   # (path, reason)

    for f in files:
        try:
            meta = parse_filename(f.name)
        except ValueError as e:
            invalid.append((f, str(e)))
            continue

        status = get_registry_status(f.name)

        if status == "CURATED" and not args.force:
            to_skip.append((f, meta, status))
        else:
            to_trigger.append((f, meta, status))

    # ── Summary table ─────────────────────────────────────────
    col_w = 48
    print(f"  {'FILE':<{col_w}} {'STATUS':<20} {'ACTION'}")
    print(f"  {'-'*col_w} {'-'*20} {'-'*20}")

    for f, meta, status in to_trigger:
        status_str = status or "NOT REGISTERED"
        print(f"  {f.name:<{col_w}} {status_str:<20} {GREEN}TRIGGER{RESET}")

    for f, meta, status in to_skip:
        print(f"  {f.name:<{col_w}} {status:<20} {YELLOW}SKIP (CURATED){RESET}")

    for f, reason in invalid:
        print(f"  {f.name:<{col_w}} {'—':<20} {RED}INVALID → QUARANTINE{RESET}")

    print()
    print(f"  {GREEN}{len(to_trigger)} to trigger{RESET}  |  "
          f"{YELLOW}{len(to_skip)} skipped{RESET}  |  "
          f"{RED}{len(invalid)} invalid{RESET}\n")

    if args.dry_run:
        print(f"{YELLOW}Dry run complete — no changes made.{RESET}\n")
        return

    # ── Handle invalid files ──────────────────────────────────
    if invalid:
        print(f"{RED}Processing {len(invalid)} invalid file(s) → quarantine...{RESET}")
        for f, reason in invalid:
            print(f"\n  {RED}Invalid: {f.name}{RESET}")
            print(f"  Reason: {reason}")
            upload_invalid_file(f, reason)
        print()

    if not to_trigger:
        print(f"{GREEN}Nothing to recover — all valid files accounted for.{RESET}\n")
        return

    # ── Process valid files per-file ──────────────────────────
    triggered = 0
    failed    = 0
    skipped   = 0

    for f, meta, status in to_trigger:
        print(f"{CYAN}File: {f.name}{RESET}")
        print(f"  Client: {meta.client_code} | Type: {meta.file_type} | "
              f"Env: {meta.env} | Date: {meta.date} | Seq: {meta.sequence}")
        print(f"  Registry status: {status or 'NOT REGISTERED'}")

        # Per-file confirmation unless single file mode
        if not args.file_name:
            confirm = input("  Trigger this file? [y/N/q to quit] ").strip().lower()
            if confirm == "q":
                print("Aborted.")
                break
            elif confirm != "y":
                print(f"  {YELLOW}Skipped.{RESET}\n")
                skipped += 1
                continue

        # Upload to MinIO raw
        try:
            upload_valid_file(meta, f)
        except Exception as e:
            logger.error("  MinIO upload failed: %s", e)
            failed += 1
            print()
            continue

        # Trigger DAG
        try:
            dag_run_id = trigger_dag(meta)
            print(f"  {GREEN}DAG triggered — dag_run_id: {dag_run_id} ✅{RESET}")
            triggered += 1
        except Exception as e:
            logger.error("  DAG trigger failed: %s", e)
            failed += 1

        print()

    # ── Final summary ─────────────────────────────────────────
    print(f"{BOLD}{'='*62}{RESET}")
    print(f"  {GREEN}Triggered: {triggered}{RESET}  |  "
          f"{YELLOW}Skipped: {skipped}{RESET}  |  "
          f"{RED}Failed: {failed}{RESET}  |  "
          f"{RED}Quarantined: {len(invalid)}{RESET}")
    print(f"{BOLD}{'='*62}{RESET}\n")

    if failed > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()

"""
# See what would trigger without doing anything
python scripts/recover_unprocessed.py --dry-run

# Recover all unprocessed files (asks for confirmation)
python scripts/recover_unprocessed.py

# Recover one specific file
python scripts/recover_unprocessed.py --file-name BCBS001_837P_PROD_20260312_001.csv

# Force re-trigger even if already CURATED
python scripts/recover_unprocessed.py --force
"""