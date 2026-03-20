from __future__ import annotations

"""
file_watcher.py — watches data/inbound/ for new CSV files,
validates the filename convention, uploads to MinIO, and
triggers the Airflow claims_pipeline DAG via REST API.

Valid files:
  → uploaded to s3a://healthcare-raw/{partition}/
  → deleted from data/inbound/ after successful upload
  → DAG triggered

Invalid files:
  → uploaded to s3a://healthcare-quarantine/hold/{timestamp}_{filename}
  → deleted from data/inbound/ after successful upload
  → no DAG trigger
  → fallback: moved to data/hold/ if MinIO is unreachable

Run (foreground):
    python scripts/file_watcher.py

Run (background):
    nohup python scripts/file_watcher.py > logs/file_watcher.log 2>&1 &

Stop background process:
    kill $(cat logs/file_watcher.pid)
"""

import logging
import os
import shutil
import sys
import time
from datetime import datetime
from pathlib import Path

import psycopg2
import requests
from minio import Minio
from watchdog.events import FileCreatedEvent, FileSystemEventHandler
from watchdog.observers import Observer

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
LOGS_DIR          = PROJECT_ROOT / "logs"

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


# ── Helpers ──────────────────────────────────────────────────
def get_minio_client() -> Minio:
    endpoint = MINIO_ENDPOINT.replace("http://", "").replace("https://", "")
    return Minio(
        endpoint,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False,
    )


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


def is_already_processed(file_name: str) -> bool:
    """Returns True if file_registry shows CURATED — skip re-processing."""
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
        return row is not None and row[0] == "CURATED"
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def upload_valid_file(meta: FileMeta, local_path: Path) -> None:
    """
    Upload valid file to s3a://healthcare-raw/{partition}/{filename}
    then delete from local inbound.
    """
    client = get_minio_client()
    ensure_bucket(client, RAW_BUCKET)
    object_key = f"{meta.partition}/{meta.file_name}"

    client.fput_object(
        bucket_name=RAW_BUCKET,
        object_name=object_key,
        file_path=str(local_path),
        content_type="text/csv",
    )
    logger.info("Uploaded → s3a://%s/%s ✅", RAW_BUCKET, object_key)

    # Delete local — MinIO is the system of record
    local_path.unlink()
    logger.info("Deleted local file after upload: %s", local_path.name)


def upload_invalid_file(local_path: Path, reason: str) -> None:
    """
    Upload invalid file to s3a://healthcare-quarantine/hold/{timestamp}_{filename}
    then delete from local inbound.
    Fallback: move to data/hold/ if MinIO is unreachable.
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
            "Invalid file → s3a://%s/%s | Reason: %s",
            QUARANTINE_BUCKET, object_key, reason,
        )

        # Delete local after successful MinIO upload
        local_path.unlink()
        logger.info("Deleted local invalid file after quarantine upload: %s", local_path.name)

    except Exception as e:
        # MinIO unreachable — fall back to local hold folder
        logger.error(
            "MinIO quarantine upload failed (%s) — falling back to data/hold/", e
        )
        HOLD_DIR.mkdir(parents=True, exist_ok=True)
        dest = HOLD_DIR / local_path.name
        shutil.move(str(local_path), str(dest))
        logger.warning("Fallback: moved to local hold: %s | Reason: %s", dest, reason)


def trigger_dag(meta: FileMeta) -> str:
    """Trigger Airflow DAG via REST API. Returns dag_run_id."""
    url = f"{AIRFLOW_BASE_URL}/api/v1/dags/{DAG_ID}/dagRuns"
    response = requests.post(
        url,
        json={"conf": {"file_name": meta.file_name}},
        auth=(AIRFLOW_USER, AIRFLOW_PASSWORD),
        timeout=30,
    )
    response.raise_for_status()
    return response.json().get("dag_run_id", "unknown")


def wait_for_file_stable(path: Path, stable_secs: float = 1.0) -> None:
    """Wait until file size stops changing — guards against partial writes."""
    prev_size = -1
    while True:
        try:
            curr_size = path.stat().st_size
        except FileNotFoundError:
            return
        if curr_size == prev_size:
            return
        prev_size = curr_size
        time.sleep(stable_secs)


# ── Core processing logic (shared by watcher + startup scan) ─
def process_file(local_path: Path) -> None:
    """
    Full processing flow for a single file.
    Called by both on_created event and startup scan.
    """
    logger.info("Processing: %s", local_path.name)

    # Wait for file to finish writing
    wait_for_file_stable(local_path)

    # ── Validate filename ─────────────────────────────────────
    try:
        meta = parse_filename(local_path.name)
    except ValueError as e:
        logger.warning("Invalid filename: %s | %s", local_path.name, e)
        upload_invalid_file(local_path, str(e))
        return

    logger.info(
        "Parsed — client: %s | type: %s | env: %s | date: %s | seq: %s",
        meta.client_code, meta.file_type, meta.env, meta.date, meta.sequence,
    )

    # ── Check if already processed ────────────────────────────
    try:
        if is_already_processed(meta.file_name):
            logger.warning(
                "File %s already CURATED — skipping. "
                "Use recover_unprocessed.py --force to re-trigger.",
                meta.file_name,
            )
            return
    except Exception as e:
        logger.error("Could not check file_registry: %s — proceeding", e)

    # ── Upload valid file to MinIO raw ────────────────────────
    try:
        upload_valid_file(meta, local_path)
    except Exception as e:
        logger.error("MinIO upload failed for %s: %s", meta.file_name, e)
        return

    # ── Trigger DAG ───────────────────────────────────────────
    try:
        dag_run_id = trigger_dag(meta)
        logger.info(
            "DAG triggered for %s | dag_run_id: %s ✅",
            meta.file_name, dag_run_id,
        )
    except Exception as e:
        logger.error("DAG trigger failed for %s: %s", meta.file_name, e)
        return

    logger.info("Done: %s ✅\n", meta.file_name)


# ── Event Handler ────────────────────────────────────────────
class ClaimsFileHandler(FileSystemEventHandler):

    def on_created(self, event: FileCreatedEvent) -> None:
        if event.is_directory:
            return

        local_path = Path(event.src_path)

        if local_path.suffix.lower() != ".csv":
            logger.debug("Ignoring non-CSV file: %s", local_path.name)
            return

        logger.info("New file detected: %s", local_path.name)
        process_file(local_path)


# ── Entry Point ──────────────────────────────────────────────
def main() -> None:
    INBOUND_DIR.mkdir(parents=True, exist_ok=True)
    HOLD_DIR.mkdir(parents=True, exist_ok=True)
    LOGS_DIR.mkdir(parents=True, exist_ok=True)

    # Write PID for background process management
    pid_file = LOGS_DIR / "file_watcher.pid"
    pid_file.write_text(str(os.getpid()))

    logger.info("=" * 60)
    logger.info("Healthcare File Watcher started")
    logger.info("Watching:         %s", INBOUND_DIR)
    logger.info("Hold (fallback):  %s", HOLD_DIR)
    logger.info("Raw bucket:       s3a://%s/", RAW_BUCKET)
    logger.info("Quarantine:       s3a://%s/hold/", QUARANTINE_BUCKET)
    logger.info("Airflow:          %s / dag: %s", AIRFLOW_BASE_URL, DAG_ID)
    logger.info("PID:              %s → %s", os.getpid(), pid_file)
    logger.info("=" * 60)

    # ── Startup scan ─────────────────────────────────────────
    # Process any files already sitting in inbound when watcher starts
    existing = sorted(INBOUND_DIR.glob("*.csv"))
    if existing:
        logger.info(
            "Startup scan: found %s existing file(s) in inbound — processing...",
            len(existing),
        )
        for f in existing:
            process_file(f)
    else:
        logger.info("Startup scan: no existing files in inbound.")

    logger.info("Watching for new files... (Ctrl+C to stop)\n")

    event_handler = ClaimsFileHandler()
    observer = Observer()
    observer.schedule(event_handler, str(INBOUND_DIR), recursive=False)
    observer.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Shutting down file watcher...")
        observer.stop()

    observer.join()
    pid_file.unlink(missing_ok=True)
    logger.info("File watcher stopped.")


if __name__ == "__main__":
    main()