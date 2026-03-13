from datetime import datetime, timezone, timedelta
import logging

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from minio import Minio

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

        client.stat_object(RAW_BUCKET, RAW_FILENAME)
        logger.info("File %s found in %s ✅", RAW_FILENAME, RAW_BUCKET)
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

        return len(objects)

    validate_output_task = PythonOperator(
        task_id="validate_output",
        python_callable=validate_output,
    )

    check_file_task >> run_cleanse_task >> validate_output_task