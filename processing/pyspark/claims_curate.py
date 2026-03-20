from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from datetime import datetime, timezone

import psycopg2
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, current_timestamp, lit

# ── Path Utils ───────────────────────────────────────────────
sys.path.insert(0, "/opt/airflow/processing")
from common.path_utils import (
    FileMeta,
    parse_filename,
    transformed_output_path,
    curated_member_summary_path,
    curated_payer_summary_path,
    curated_provider_summary_path,
)


# ── Logging ──────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ── Constants ────────────────────────────────────────────────
JOB_NAME = "claims_curate_job"

POSTGRES_HOST     = os.getenv("POSTGRES_HOST",     "postgres")
POSTGRES_PORT     = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB       = os.getenv("POSTGRES_DB",       "pipeline_db")
POSTGRES_USER     = os.getenv("POSTGRES_USER",     "pgadmin")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "pgpassword123")

MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT",   "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin123")


# ── Args ─────────────────────────────────────────────────────
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Claims curate job")
    parser.add_argument(
        "--file-name",
        required=True,
        help="e.g. BCBS001_837P_PROD_20260312_001.csv",
    )
    return parser.parse_args()


# ── Spark ────────────────────────────────────────────────────
def build_spark() -> SparkSession:
    builder = (
        SparkSession.builder
        .appName("Claims-Curate-Job")
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.endpoint",               MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key",             MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key",             MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access",      "true")
        .config("spark.hadoop.fs.s3a.impl",                   "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.connection.timeout",     "600000")
        .config("spark.hadoop.fs.s3a.socket.timeout",         "600000")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ── Postgres Logging ─────────────────────────────────────────
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
            host=POSTGRES_HOST, port=POSTGRES_PORT,
            database=POSTGRES_DB, user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
        )
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO pipeline_run (
                pipeline_name, run_status, started_at, ended_at,
                records_read, records_written, records_rejected, error_message
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (JOB_NAME, status, run_time, datetime.now(timezone.utc),
             records_read, records_written, records_rejected, error_msg),
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

    meta: FileMeta = parse_filename(args.file_name)

    input_path    = transformed_output_path(meta)
    member_path   = curated_member_summary_path(meta)
    payer_path    = curated_payer_summary_path(meta)
    provider_path = curated_provider_summary_path(meta)

    logger.info("Starting claims curate job")
    logger.info("File:                  %s", meta.file_name)
    logger.info("Client:                %s", meta.client_code)
    logger.info("Env:                   %s", meta.env)
    logger.info("Input path:            %s", input_path)
    logger.info("Member summary path:   %s", member_path)
    logger.info("Payer summary path:    %s", payer_path)
    logger.info("Provider summary path: %s", provider_path)

    run_time = datetime.now(timezone.utc)
    started  = time.time()

    raw_count       = 0
    records_written = 0

    spark = build_spark()
    logger.info("Spark session with Delta created successfully")

    try:
        # ── Read Transformed Claims ──────────────────────────
        logger.info("Reading transformed claims from %s", input_path)
        transformed_df = spark.read.format("delta").load(input_path)

        raw_count = transformed_df.count()
        logger.info("Transformed record count: %s", raw_count)

        # Cache — reused by all 3 aggregations
        transformed_df.cache()

        # ── Member Summary ───────────────────────────────────
        logger.info("Building member summary...")
        member_summary_df = (
            transformed_df
            .groupBy("member_id_hash")
            .agg(
                F.count("claim_id").alias("total_claims"),
                F.round(F.sum("billed_amount"), 2).alias("total_billed"),
                F.round(F.avg("billed_amount"), 2).alias("avg_billed"),
                F.countDistinct("provider_npi_hash").alias("unique_providers"),
                F.countDistinct("payer_id").alias("unique_payers"),
                F.max(
                    F.when(col("chronic_flag") == True, True).otherwise(False)
                ).alias("has_chronic_condition"),
                F.max("service_date").alias("most_recent_service_date"),
            )
            .withColumn("curated_at", current_timestamp())
        )
        member_count = member_summary_df.count()
        logger.info("Member summary rows: %s", member_count)
        member_summary_df.show(5, truncate=False)

        # ── Payer Summary ────────────────────────────────────
        logger.info("Building payer summary...")
        payer_summary_df = (
            transformed_df
            .groupBy("payer_id")
            .agg(
                F.count("claim_id").alias("total_claims"),
                F.round(F.sum("billed_amount"), 2).alias("total_billed"),
                F.round(F.avg("billed_amount"), 2).alias("avg_billed"),
                F.countDistinct("member_id_hash").alias("unique_members"),
                F.countDistinct("provider_npi_hash").alias("unique_providers"),
                F.round(F.avg("claim_age_days"), 1).alias("avg_claim_age_days"),
                F.count(F.when(col("billed_category") == "LOW",    True)).alias("low_claims"),
                F.count(F.when(col("billed_category") == "MEDIUM", True)).alias("medium_claims"),
                F.count(F.when(col("billed_category") == "HIGH",   True)).alias("high_claims"),
            )
            .withColumn("curated_at", current_timestamp())
        )
        payer_count = payer_summary_df.count()
        logger.info("Payer summary rows: %s", payer_count)
        payer_summary_df.show(5, truncate=False)

        # ── Provider Summary ─────────────────────────────────
        logger.info("Building provider summary...")
        provider_summary_df = (
            transformed_df
            .groupBy("provider_npi_hash")
            .agg(
                F.count("claim_id").alias("total_claims"),
                F.round(F.sum("billed_amount"), 2).alias("total_billed"),
                F.round(F.avg("billed_amount"), 2).alias("avg_billed"),
                F.countDistinct("member_id_hash").alias("unique_members"),
                F.countDistinct("payer_id").alias("unique_payers"),
                F.countDistinct("cpt_code").alias("unique_cpt_codes"),
            )
            .withColumn("curated_at", current_timestamp())
        )
        provider_count = provider_summary_df.count()
        logger.info("Provider summary rows: %s", provider_count)
        provider_summary_df.show(5, truncate=False)

        records_written = member_count + payer_count + provider_count

        # ── Write as Delta ───────────────────────────────────
        for df, path, label in [
            (member_summary_df,   member_path,   "member"),
            (payer_summary_df,    payer_path,    "payer"),
            (provider_summary_df, provider_path, "provider"),
        ]:
            logger.info("Writing %s summary to %s", label, path)
            (
                df.write
                .format("delta")
                .mode("overwrite")
                .option("overwriteSchema", "true")
                .save(path)
            )

        logger.info("All curated summaries written as Delta ✅")

        transformed_df.unpersist()

        elapsed = round(time.time() - started, 2)
        logger.info("Claims curate job complete in %ss ✅", elapsed)

        log_to_postgres(
            status="SUCCESS",
            run_time=run_time,
            records_read=raw_count,
            records_written=records_written,
        )

    except Exception as e:
        logger.exception("Claims curate job failed for file: %s", meta.file_name)
        try:
            log_to_postgres(
                status="FAILED",
                run_time=run_time,
                records_read=raw_count,
                records_written=records_written,
                error_msg=str(e),
            )
        except Exception:
            logger.exception("Failed to log failure to PostgreSQL")
        raise

    finally:
        spark.stop()
        logger.info("Spark session stopped")


if __name__ == "__main__":
    main()