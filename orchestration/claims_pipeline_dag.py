from datetime import datetime, timezone, timedelta
import logging

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from minio import Minio
import psycopg2

logger = logging.getLogger(__name__)

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

RAW_BUCKET = "healthcare-raw"
RAW_FILENAME = "BCBS001_837P_20260312.csv"
CLEANSED_BUCKET = "healthcare-cleansed"


def get_minio_client() -> Minio:
    return Minio(
        "minio:9000",
        access_key="minioadmin",
        secret_key="minioadmin123",
        secure=False,
    )


def get_postgres_connection():
    return psycopg2.connect(
        host="postgres",
        port=5432,
        database="pipeline_db",
        user="pgadmin",
        password="pgpassword123",
    )


with DAG(
    dag_id="claims_pipeline",
    default_args=default_args,
    description="Healthcare Claims Processing Pipeline",
    schedule="0 2 * * *",
    start_date=datetime(2026, 3, 13, tzinfo=timezone.utc),
    catchup=False,
    tags=["healthcare", "claims", "pipeline"],
) as dag:

    def check_file_exists():
        client = get_minio_client()

        if not client.bucket_exists(RAW_BUCKET):
            raise Exception(f"Bucket {RAW_BUCKET} does not exist!")

        obj = client.stat_object(RAW_BUCKET, RAW_FILENAME)
        logger.info("File %s found in %s ✅", RAW_FILENAME, RAW_BUCKET)

        conn = None
        cursor = None
        try:
            conn = get_postgres_connection()
            cursor = conn.cursor()

            cursor.execute(
                """
                SELECT file_id
                FROM file_registry
                WHERE bucket_name = %s
                  AND object_key = %s
                """,
                (RAW_BUCKET, RAW_FILENAME),
            )
            existing = cursor.fetchone()

            if existing:
                logger.info(
                    "File already registered in file_registry with file_id=%s",
                    existing[0],
                )
            else:
                cursor.execute(
                    """
                    INSERT INTO file_registry (
                        source_name,
                        file_name,
                        bucket_name,
                        object_key,
                        file_size_bytes,
                        ingestion_status,
                        received_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, NOW())
                    """,
                    (
                        "BCBS",
                        RAW_FILENAME,
                        RAW_BUCKET,
                        RAW_FILENAME,
                        obj.size,
                        "RECEIVED",
                    ),
                )
                conn.commit()
                logger.info("File registered in file_registry ✅")
        finally:
            if cursor is not None:
                cursor.close()
            if conn is not None:
                conn.close()

        return RAW_FILENAME

    check_file_task = PythonOperator(
        task_id="check_file_exists",
        python_callable=check_file_exists,
    )

    run_cleanse_task = BashOperator(
        task_id="run_claims_cleanse",
        bash_command="""
            set -e
            spark-submit \
                --master local[*] \
                --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
                /opt/airflow/processing/pyspark/claims_cleanse.py
        """,
    )

    def validate_output():
        client = get_minio_client()

        if not client.bucket_exists(CLEANSED_BUCKET):
            raise Exception(f"Bucket {CLEANSED_BUCKET} does not exist!")

        objects = list(client.list_objects(CLEANSED_BUCKET, recursive=True))
        if not objects:
            raise Exception(f"No output files found in {CLEANSED_BUCKET}!")

        logger.info("Found %s output files in %s ✅", len(objects), CLEANSED_BUCKET)
        for obj in objects:
            logger.info("  → %s", obj.object_name)

        conn = None
        cursor = None
        try:
            conn = get_postgres_connection()
            cursor = conn.cursor()
            cursor.execute(
                """
                UPDATE file_registry
                SET ingestion_status = %s,
                    processed_at = NOW(),
                    error_message = NULL
                WHERE bucket_name = %s
                  AND object_key = %s
                """,
                ("CLEANSED", RAW_BUCKET, RAW_FILENAME),
            )
            conn.commit()
            logger.info("file_registry updated to CLEANSED ✅")
        finally:
            if cursor is not None:
                cursor.close()
            if conn is not None:
                conn.close()

        return len(objects)

    validate_output_task = PythonOperator(
        task_id="validate_output",
        python_callable=validate_output,
    )

    check_file_task >> run_cleanse_task >> validate_output_task