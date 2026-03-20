from __future__ import annotations

import logging
import os
import sys
from datetime import datetime, timedelta, timezone

import psycopg2
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from minio import Minio

# ── Path Utils ───────────────────────────────────────────────
sys.path.insert(0, "/opt/airflow/processing")
from common.path_utils import (
    FileMeta,
    parse_filename,
)

logger = logging.getLogger(__name__)

# ── Constants ────────────────────────────────────────────────
RAW_BUCKET         = "healthcare-raw"
CLEANSED_BUCKET    = "healthcare-cleansed"
TRANSFORMED_BUCKET = "healthcare-transformed"
CURATED_BUCKET     = "healthcare-curated"

MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT",   "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin123")

POSTGRES_HOST     = os.getenv("POSTGRES_HOST",     "postgres")
POSTGRES_PORT     = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB       = os.getenv("POSTGRES_DB",       "pipeline_db")
POSTGRES_USER     = os.getenv("POSTGRES_USER",     "pgadmin")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "pgpassword123")


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


def get_meta(**context) -> FileMeta:
    """Parse FileMeta from dag_run.conf — fail fast if missing or invalid."""
    file_name = context["dag_run"].conf.get("file_name")
    if not file_name:
        raise ValueError(
            "dag_run.conf['file_name'] is required.\n"
            "Example: {\"file_name\": \"BCBS001_837P_PROD_20260312_001.csv\"}"
        )
    return parse_filename(file_name)


def update_file_registry(conn, status: str, file_name: str, error_msg: str | None = None) -> None:
    cursor = None
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            UPDATE file_registry
            SET ingestion_status = %s,
                processed_at = NOW(),
                error_message = %s
            WHERE bucket_name = %s AND object_key = %s
            """,
            (status, error_msg, RAW_BUCKET, file_name),
        )
        conn.commit()
        logger.info("file_registry updated to %s ✅", status)
    finally:
        if cursor is not None:
            cursor.close()


def check_parquet_files(client: Minio, bucket: str, prefix: str) -> int:
    """Returns count of parquet files under prefix. Raises if none found."""
    objects = list(client.list_objects(bucket, prefix=prefix, recursive=True))
    parquet_files = [o for o in objects if o.object_name.endswith(".parquet")]
    if not parquet_files:
        raise Exception(f"No parquet files found in {bucket}/{prefix}")
    logger.info("Found %s parquet files in %s/%s ✅", len(parquet_files), bucket, prefix)
    return len(parquet_files)


# ── DAG ──────────────────────────────────────────────────────
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="claims_pipeline",
    default_args=default_args,
    description="Healthcare Claims Processing Pipeline",
    schedule="0 2 * * *",
    start_date=datetime(2026, 3, 13, tzinfo=timezone.utc),
    catchup=False,
    tags=["healthcare", "claims", "pipeline"],
) as dag:

    # ── Task 1: Check File Exists ─────────────────────────────
    def check_file_exists(**context) -> str:
        meta = get_meta(**context)
        client = get_minio_client()
        raw_object_key = f"{meta.partition}/{meta.file_name}"

        if not client.bucket_exists(RAW_BUCKET):
            raise Exception(f"Bucket {RAW_BUCKET} does not exist!")

        obj = client.stat_object(RAW_BUCKET, raw_object_key)
        logger.info("File %s found in %s ✅", raw_object_key, RAW_BUCKET)

        conn = None
        cursor = None
        try:
            conn = get_postgres_connection()
            cursor = conn.cursor()
            cursor.execute(
                "SELECT file_id, ingestion_status FROM file_registry WHERE bucket_name = %s AND object_key = %s",
                (RAW_BUCKET, meta.file_name),
            )
            existing = cursor.fetchone()

            if existing:
                logger.info(
                    "File already registered with file_id=%s status=%s",
                    existing[0], existing[1],
                )
            else:
                cursor.execute(
                    """
                    INSERT INTO file_registry (
                        source_name, file_name, bucket_name, object_key,
                        file_size_bytes, ingestion_status, received_at,
                        client_code, file_type, env, sequence
                    ) VALUES (%s, %s, %s, %s, %s, %s, NOW(), %s, %s, %s, %s)
                    """,
                    (
                        meta.client_code, meta.file_name, RAW_BUCKET, meta.file_name,
                        obj.size, "RECEIVED",
                        meta.client_code, meta.file_type, meta.env, meta.sequence,
                    ),
                )
                conn.commit()
                logger.info("File registered in file_registry ✅")
        finally:
            if cursor is not None:
                cursor.close()
            if conn is not None:
                conn.close()

        return meta.file_name

    check_file_task = PythonOperator(
        task_id="check_file_exists",
        python_callable=check_file_exists,
    )

    # ── Task 2: Run Cleanse ───────────────────────────────────
    run_cleanse_task = BashOperator(
        task_id="run_claims_cleanse",
        bash_command="""
            set -e
            spark-submit \
                --master local[*] \
                --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
                /opt/airflow/processing/pyspark/claims_cleanse.py \
                --file-name {{ dag_run.conf['file_name'] }}
        """,
    )

    # ── Task 3: Validate Cleansed Output ──────────────────────
    def validate_cleansed_output(**context) -> int:
        meta = get_meta(**context)
        prefix = f"claims/{meta.partition}/"
        client = get_minio_client()
        conn = None
        try:
            if not client.bucket_exists(CLEANSED_BUCKET):
                raise Exception(f"Bucket {CLEANSED_BUCKET} does not exist!")
            count = check_parquet_files(client, CLEANSED_BUCKET, prefix)
            conn = get_postgres_connection()
            update_file_registry(conn, "CLEANSED", meta.file_name)
            return count
        except Exception as e:
            try:
                conn = get_postgres_connection()
                update_file_registry(conn, "FAILED", meta.file_name, str(e))
            except Exception:
                logger.exception("Failed to update file_registry on failure")
            raise
        finally:
            if conn is not None:
                conn.close()

    validate_cleansed_task = PythonOperator(
        task_id="validate_cleansed_output",
        python_callable=validate_cleansed_output,
    )

    # ── Task 4: Run Transform ─────────────────────────────────
    run_transform_task = BashOperator(
        task_id="run_claims_transform",
        bash_command="""
            set -e
            spark-submit \
                --master local[*] \
                --packages io.delta:delta-spark_2.12:3.0.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
                --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
                --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
                /opt/airflow/processing/pyspark/claims_transform.py \
                --file-name {{ dag_run.conf['file_name'] }}
        """,
    )

    # ── Task 5: Validate Transformed Output ───────────────────
    def validate_transformed_output(**context) -> int:
        meta = get_meta(**context)
        prefix = f"claims/{meta.partition}/"
        client = get_minio_client()
        conn = None
        try:
            if not client.bucket_exists(TRANSFORMED_BUCKET):
                raise Exception(f"Bucket {TRANSFORMED_BUCKET} does not exist!")
            count = check_parquet_files(client, TRANSFORMED_BUCKET, prefix)
            conn = get_postgres_connection()
            update_file_registry(conn, "TRANSFORMED", meta.file_name)
            return count
        except Exception as e:
            try:
                conn = get_postgres_connection()
                update_file_registry(conn, "FAILED", meta.file_name, str(e))
            except Exception:
                logger.exception("Failed to update file_registry on failure")
            raise
        finally:
            if conn is not None:
                conn.close()

    validate_transformed_task = PythonOperator(
        task_id="validate_transformed_output",
        python_callable=validate_transformed_output,
    )

    # ── Task 6: Run Curate ────────────────────────────────────
    run_curate_task = BashOperator(
        task_id="run_claims_curate",
        bash_command="""
            set -e
            spark-submit \
                --master local[*] \
                --packages io.delta:delta-spark_2.12:3.0.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
                --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
                --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
                /opt/airflow/processing/pyspark/claims_curate.py \
                --file-name {{ dag_run.conf['file_name'] }}
        """,
    )

    # ── Task 7: Validate Curated Output ──────────────────────
    def validate_curated_output(**context) -> int:
        meta = get_meta(**context)
        client = get_minio_client()
        conn = None
        try:
            if not client.bucket_exists(CURATED_BUCKET):
                raise Exception(f"Bucket {CURATED_BUCKET} does not exist!")

            total = 0
            for summary in ["member_summary", "payer_summary", "provider_summary"]:
                prefix = f"{summary}/{meta.partition}/"
                total += check_parquet_files(client, CURATED_BUCKET, prefix)

            conn = get_postgres_connection()
            update_file_registry(conn, "CURATED", meta.file_name)
            return total

        except Exception as e:
            try:
                conn = get_postgres_connection()
                update_file_registry(conn, "FAILED", meta.file_name, str(e))
            except Exception:
                logger.exception("Failed to update file_registry on failure")
            raise
        finally:
            if conn is not None:
                conn.close()

    validate_curated_task = PythonOperator(
        task_id="validate_curated_output",
        python_callable=validate_curated_output,
    )

    # ── Pipeline Flow ─────────────────────────────────────────
    (
        check_file_task
        >> run_cleanse_task
        >> validate_cleansed_task
        >> run_transform_task
        >> validate_transformed_task
        >> run_curate_task
        >> validate_curated_task
    )