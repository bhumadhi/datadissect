from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timezone, timedelta
from minio import Minio
import psycopg2
import logging

logger = logging.getLogger(__name__)

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
    schedule_interval="0 2 * * *",
    start_date=datetime(2026, 3, 13, tzinfo=timezone.utc),
    catchup=False,
    tags=["healthcare", "claims", "pipeline"],
) as dag:

    def check_file_exists():
        client = Minio(
            "minio:9000",
            access_key="minioadmin",
            secret_key="minioadmin123",
            secure=False
        )
        bucket = "healthcare-raw"
        filename = "BCBS001_837P_20260312.csv"
        found = client.bucket_exists(bucket)
        if not found:
            raise Exception(f"Bucket {bucket} does not exist!")
        objects = list(client.list_objects(bucket))
        filenames = [obj.object_name for obj in objects]
        if filename not in filenames:
            raise Exception(f"File {filename} not found in {bucket}!")
        logger.info(f"File {filename} found in {bucket} ✅")
        return filename

    check_file_task = PythonOperator(
        task_id="check_file_exists",
        python_callable=check_file_exists,
    )

    run_cleanse_task = BashOperator(
        task_id="run_claims_cleanse",
        bash_command="""
            spark-submit \
                --master local[*] \
                --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
                /opt/airflow/processing/pyspark/claims_cleanse.py
        """,
    )

    def validate_output():
        client = Minio(
            "minio:9000",
            access_key="minioadmin",
            secret_key="minioadmin123",
            secure=False
        )
        bucket = "healthcare-cleansed"
        objects = list(client.list_objects(bucket, recursive=True))
        if len(objects) == 0:
            raise Exception("No output files found in healthcare-cleansed!")
        logger.info(f"Found {len(objects)} output files in {bucket} ✅")
        for obj in objects:
            logger.info(f"  → {obj.object_name}")
        return len(objects)

    validate_output_task = PythonOperator(
        task_id="validate_output",
        python_callable=validate_output,
    )

    def notify_completion(**context):
        conn = psycopg2.connect(
            host="postgres",
            port=5432,
            database="pipeline_db",
            user="pgadmin",
            password="pgpassword123"
        )
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE pipeline_run
            SET run_status = 'SUCCESS',
                ended_at = NOW()
            WHERE pipeline_name = 'claims_cleanse_job'
            AND run_status = 'RUNNING'
        """)
        conn.commit()
        cursor.close()
        conn.close()
        logger.info("Pipeline completion logged ✅")

    notify_task = PythonOperator(
        task_id="notify_completion",
        python_callable=notify_completion,
        provide_context=True,
    )

    check_file_task >> run_cleanse_task >> validate_output_task >> notify_task