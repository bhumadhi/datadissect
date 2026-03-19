from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from datetime import datetime, timezone

import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, current_timestamp, lit, sha2

# ── Path Utils ───────────────────────────────────────────────
# Ensure common/ is importable when run via spark-submit inside Airflow container
sys.path.insert(0, "/opt/airflow/processing")
from common.path_utils import (
    cleansed_output_path,
    quarantine_output_path,
    raw_input_path,
)


# ── Logging Setup ────────────────────────────────────────────
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ── Constants ────────────────────────────────────────────────
JOB_NAME = "claims_cleanse_job"

# Postgres — env vars with local fallback defaults
POSTGRES_HOST     = os.getenv("POSTGRES_HOST",     "postgres")
POSTGRES_PORT     = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB       = os.getenv("POSTGRES_DB",       "pipeline_db")
POSTGRES_USER     = os.getenv("POSTGRES_USER",     "pgadmin")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "pgpassword123")

# MinIO — env vars with local fallback defaults
MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT",   "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin123")


# ── Args ─────────────────────────────────────────────────────
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Claims cleanse job")
    parser.add_argument(
        "--file-name",
        required=True,
        help="Raw input filename in MinIO (e.g. BCBS001_837P_20260312.csv)",
    )
    return parser.parse_args()


# ── Spark Session ────────────────────────────────────────────
def build_spark() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("Claims-Cleanse-Job")
        .config("spark.hadoop.fs.s3a.endpoint",               MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key",             MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key",             MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access",      "true")
        .config("spark.hadoop.fs.s3a.impl",                   "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.connection.timeout",     "600000")
        .config("spark.hadoop.fs.s3a.socket.timeout",         "600000")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ── PostgreSQL Logging ───────────────────────────────────────
def log_to_postgres(
    status: str,
    run_time: datetime,
    records_read: int = 0,
    records_written: int = 0,
    records_rejected: int = 0,
    error_msg: str | None = None,
) -> None:
    conn = None
    cursor = None
    try:
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            database=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
        )
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO pipeline_run (
                pipeline_name,
                run_status,
                started_at,
                ended_at,
                records_read,
                records_written,
                records_rejected,
                error_message
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                JOB_NAME,
                status,
                run_time,
                datetime.now(timezone.utc),
                records_read,
                records_written,
                records_rejected,
                error_msg,
            ),
        )
        conn.commit()
        logger.info("Pipeline run logged to PostgreSQL with status=%s", status)
    finally:
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()


# ── Main ─────────────────────────────────────────────────────
def main() -> None:
    args = parse_args()
    file_name = args.file_name

    input_path      = raw_input_path(file_name)
    clean_path      = cleansed_output_path(file_name)
    quarantine_path = quarantine_output_path(file_name)

    logger.info("Starting claims cleanse job for file: %s", file_name)
    logger.info("Input path:      %s", input_path)
    logger.info("Cleansed path:   %s", clean_path)
    logger.info("Quarantine path: %s", quarantine_path)

    run_time = datetime.now(timezone.utc)
    started  = time.time()

    # Initialise counts so failure logging never breaks
    raw_count        = 0
    cleansed_count   = 0
    quarantine_count = 0

    spark = build_spark()
    logger.info("Spark session created successfully")

    try:
        # ── Read CSV from MinIO ──────────────────────────────
        logger.info("Reading claims from %s", input_path)
        raw_df = (
            spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .csv(input_path)
        )
        raw_count = raw_df.count()
        logger.info("Raw record count: %s", raw_count)
        raw_df.printSchema()
        raw_df.show(5, truncate=False)

        # ── Validation Rules ─────────────────────────────────
        logger.info("Applying validation rules...")
        validated_df = (
            raw_df
            .withColumn(
                "is_valid",
                col("claim_id").isNotNull()
                & col("member_id").isNotNull()
                & col("provider_npi").isNotNull()
                & col("billed_amount").isNotNull()
                & (col("billed_amount") > 0),
            )
            .withColumn(
                "validation_errors",
                F.concat_ws(
                    ", ",
                    F.when(col("claim_id").isNull(),      lit("missing claim_id")),
                    F.when(col("member_id").isNull(),     lit("missing member_id")),
                    F.when(col("provider_npi").isNull(),  lit("missing provider_npi")),
                    F.when(col("billed_amount").isNull(), lit("missing billed_amount")),
                    F.when(
                        col("billed_amount").isNotNull() & (col("billed_amount") <= 0),
                        lit("invalid billed_amount"),
                    ),
                ),
            )
        )

        clean_df = (
            validated_df
            .filter(col("is_valid"))
            .drop("is_valid", "validation_errors")
        )
        quarantine_df = (
            validated_df
            .filter(~col("is_valid"))
            .drop("is_valid")
        )

        quarantine_count = quarantine_df.count()
        logger.info("Quarantine records: %s", quarantine_count)

        # ── PHI Masking ──────────────────────────────────────
        logger.info("Applying PHI masking...")
        cleansed_df = (
            clean_df
            .withColumn("member_id_hash",    sha2(col("member_id").cast("string"),    256))
            .withColumn("provider_npi_hash", sha2(col("provider_npi").cast("string"), 256))
            .drop("member_id", "provider_npi")
            .withColumn("processed_at", current_timestamp())
            .withColumn("source_file",  lit(file_name))
        )

        cleansed_count = cleansed_df.count()
        logger.info("Cleansed records: %s", cleansed_count)
        logger.info("PHI masking complete")
        cleansed_df.show(5, truncate=False)

        # ── Write Cleansed Data ──────────────────────────────
        logger.info("Writing cleansed data to %s", clean_path)
        cleansed_df.write.mode("overwrite").parquet(clean_path)

        # ── Write Quarantine Data ────────────────────────────
        if quarantine_count > 0:
            logger.info("Writing quarantine data to %s", quarantine_path)
            quarantine_df.write.mode("overwrite").parquet(quarantine_path)

        elapsed = round(time.time() - started, 2)
        logger.info("Claims cleanse job complete in %ss ✅", elapsed)

        log_to_postgres(
            status="SUCCESS",
            run_time=run_time,
            records_read=raw_count,
            records_written=cleansed_count,
            records_rejected=quarantine_count,
        )

    except Exception as e:
        logger.exception("Claims cleanse job failed for file: %s", file_name)
        try:
            log_to_postgres(
                status="FAILED",
                run_time=run_time,
                records_read=raw_count,
                records_written=cleansed_count,
                records_rejected=quarantine_count,
                error_msg=str(e),
            )
        except Exception:
            logger.exception("Failed to log job failure to PostgreSQL")
        raise

    finally:
        spark.stop()
        logger.info("Spark session stopped")


if __name__ == "__main__":
    main()