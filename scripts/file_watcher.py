from __future__ import annotations

"""
file_watcher.py — watches data/inbound/ for new CSV files,
validates the filename convention, uploads to MinIO, and
triggers the Airflow claims_pipeline DAG via REST API.

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
# Folders (relative to project root)
PROJECT_ROOT  = Path(__file__).resolve().parent.parent
INBOUND_DIR   = PROJECT_ROOT / "data" / "inbound"
HOLD_DIR      = PROJECT_ROOT / "data" / "hold"
LOGS_DIR      = PROJECT_ROOT / "logs"

# MinIO
MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT",   "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin123")
RAW_BUCKET       = "healthcare-raw"

# Airflow REST API
AIRFLOW_BASE_URL  = os.getenv("AIRFLOW_BASE_URL",  "http://localhost:8082")
AIRFLOW_USER      = os.getenv("AIRFLOW_ADMIN_USER",     "admin")
AIRFLOW_PASSWORD  = os.getenv("AIRFLOW_ADMIN_PASSWORD", "admin123")
DAG_ID            = "claims_pipeline"

# Postgres
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


def is_already_processed(file_name: str) -> bool:
    """
    Returns True if file_registry shows this file is already CURATED.
    Skips re-processing to avoid duplicate runs.
    """
    conn = None
    cursor = None
    try:
        conn = get_postgres_connection()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT ingestion_status FROM file_registry WHERE file_name = %s ORDER BY received_at DESC LIMIT 1",
            (file_name,),
        )
        row = cursor.fetchone()
        if row and row[0] == "CURATED":
            return True
        return False
    finally:
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()


def upload_to_minio(meta: FileMeta, local_path: Path) -> str:
    """
    Upload file to MinIO at:
    healthcare-raw/{client}/{file_type}/{env}/{date}_{sequence}/{file_name}

    Returns the object key.
    """
    client = get_minio_client()
    object_key = f"{meta.partition}/{meta.file_name}"

    if not client.bucket_exists(RAW_BUCKET):
        client.make_bucket(RAW_BUCKET)
        logger.info("Created bucket: %s", RAW_BUCKET)

    client.fput_object(
        bucket_name=RAW_BUCKET,
        object_name=object_key,
        file_path=str(local_path),
        content_type="text/csv",
    )
    logger.info("Uploaded %s → s3a://%s/%s ✅", meta.file_name, RAW_BUCKET, object_key)
    return object_key


def trigger_dag(meta: FileMeta) -> str:
    """
    Trigger the Airflow claims_pipeline DAG via REST API.
    Returns the dag_run_id.
    """
    url = f"{AIRFLOW_BASE_URL}/api/v1/dags/{DAG_ID}/dagRuns"
    payload = {"conf": {"file_name": meta.file_name}}

    response = requests.post(
        url,
        json=payload,
        auth=(AIRFLOW_USER, AIRFLOW_PASSWORD),
        timeout=30,
    )
    response.raise_for_status()

    dag_run_id = response.json().get("dag_run_id", "unknown")
    logger.info("DAG triggered successfully — dag_run_id: %s ✅", dag_run_id)
    return dag_run_id


def move_to_hold(local_path: Path, reason: str) -> None:
    """Move an invalid file to data/hold/ and log the reason."""
    HOLD_DIR.mkdir(parents=True, exist_ok=True)
    dest = HOLD_DIR / local_path.name
    shutil.move(str(local_path), str(dest))
    logger.warning("File moved to hold: %s | Reason: %s", dest, reason)


def wait_for_file_stable(path: Path, stable_secs: float = 1.0) -> None:
    """
    Wait until file size stops changing — guards against picking up
    a file that's still being written.
    """
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


# ── Event Handler ────────────────────────────────────────────
class ClaimsFileHandler(FileSystemEventHandler):

    def on_created(self, event: FileCreatedEvent) -> None:
        if event.is_directory:
            return

        local_path = Path(event.src_path)

        # Only process CSV files
        if local_path.suffix.lower() != ".csv":
            logger.debug("Ignoring non-CSV file: %s", local_path.name)
            return

        logger.info("New file detected: %s", local_path.name)

        # Wait for file to finish writing
        wait_for_file_stable(local_path)

        # ── Validate filename convention ──────────────────────
        try:
            meta = parse_filename(local_path.name)
        except ValueError as e:
            move_to_hold(local_path, str(e))
            return

        logger.info(
            "Parsed — client: %s | type: %s | env: %s | date: %s | seq: %s",
            meta.client_code, meta.file_type, meta.env, meta.date, meta.sequence,
        )

        # ── Check if already processed ────────────────────────
        try:
            if is_already_processed(meta.file_name):
                logger.warning(
                    "File %s already CURATED in file_registry — skipping. "
                    "Trigger manually if re-run is intended.",
                    meta.file_name,
                )
                return
        except Exception as e:
            logger.error("Could not check file_registry: %s — proceeding with upload", e)

        # ── Upload to MinIO ───────────────────────────────────
        try:
            upload_to_minio(meta, local_path)
        except Exception as e:
            logger.error("MinIO upload failed for %s: %s", meta.file_name, e)
            return

        # ── Trigger DAG ───────────────────────────────────────
        try:
            dag_run_id = trigger_dag(meta)
            logger.info(
                "Pipeline triggered for %s | dag_run_id: %s",
                meta.file_name, dag_run_id,
            )
        except Exception as e:
            logger.error("Failed to trigger DAG for %s: %s", meta.file_name, e)
            return

        logger.info("Done processing %s ✅\n", meta.file_name)


# ── Entry Point ──────────────────────────────────────────────
def main() -> None:
    # Ensure directories exist
    INBOUND_DIR.mkdir(parents=True, exist_ok=True)
    HOLD_DIR.mkdir(parents=True, exist_ok=True)
    LOGS_DIR.mkdir(parents=True, exist_ok=True)

    # Write PID file so background process can be stopped easily
    pid_file = LOGS_DIR / "file_watcher.pid"
    pid_file.write_text(str(os.getpid()))

    logger.info("=" * 60)
    logger.info("Healthcare File Watcher started")
    logger.info("Watching:      %s", INBOUND_DIR)
    logger.info("Hold folder:   %s", HOLD_DIR)
    logger.info("MinIO:         %s / %s", MINIO_ENDPOINT, RAW_BUCKET)
    logger.info("Airflow:       %s / dag: %s", AIRFLOW_BASE_URL, DAG_ID)
    logger.info("PID:           %s (saved to %s)", os.getpid(), pid_file)
    logger.info("=" * 60)

    event_handler = ClaimsFileHandler()
    observer = Observer()
    observer.schedule(event_handler, str(INBOUND_DIR), recursive=False)
    observer.start()

    logger.info("Watching for new files... (Ctrl+C to stop)\n")

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