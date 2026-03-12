from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timezone, timedelta
from minio import Minio
import psycopg2
import logging

logger = logging.getLogger(__name__)

# ── Default Arguments ────────────────────────────────────────
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# ── DAG Definition ───────────────────────────────────────────
with DAG(
    dag_id="claims_pipeline",
    default_args=default_args,
    description="Healthcare Claims Processing Pipeline",
    schedule_interval="0 2 * * *",
    start_date=days_ago(1),
    catchup=False,
    tags=["healthcare", "claims", "pipeline"],
) as dag:
    pass  # tasks go here in next sections


