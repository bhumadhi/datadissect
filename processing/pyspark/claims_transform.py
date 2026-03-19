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
from pyspark.sql.functions import col, current_timestamp, datediff, lit, when

# ── Path Utils ───────────────────────────────────────────────
# Ensure common/ is importable when run via spark-submit inside Airflow container
sys.path.insert(0, "/opt/airflow/processing")
from common.path_utils import (
    cleansed_output_path,
    cpt_summary_output_path,
    payer_summary_output_path,
    transformed_output_path,
)


# ── Logging Setup ────────────────────────────────────────────
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ── Constants ────────────────────────────────────────────────
JOB_NAME = "claims_transform_job"

CPT_REF_PATH   = "s3a://healthcare-reference/cpt_codes.csv"
ICD10_REF_PATH = "s3a://healthcare-reference/icd10_codes.csv"

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
    parser = argparse.ArgumentParser(description="Claims transform job")
    parser.add_argument(
        "--file-name",
        required=True,
        help="Source filename (e.g. BCBS001_837P_20260312.csv) — used to resolve input/output paths",
    )
    return parser.parse_args()


# ── Spark Session with Delta ─────────────────────────────────
def build_spark() -> SparkSession:
    builder = (
        SparkSession.builder
        .appName("Claims-Transform-Job")
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

    input_path        = cleansed_output_path(file_name)
    output_path       = transformed_output_path(file_name)
    payer_summary_path = payer_summary_output_path(file_name)
    cpt_summary_path  = cpt_summary_output_path(file_name)

    logger.info("Starting claims transform job for file: %s", file_name)
    logger.info("Input path:         %s", input_path)
    logger.info("Output path:        %s", output_path)
    logger.info("Payer summary path: %s", payer_summary_path)
    logger.info("CPT summary path:   %s", cpt_summary_path)

    run_time = datetime.now(timezone.utc)
    started  = time.time()

    # Initialise counts so failure logging never breaks
    raw_count         = 0
    transformed_count = 0
    summary_count     = 0

    spark = build_spark()
    logger.info("Spark session with Delta created successfully")

    try:
        # ── Read Cleansed Claims from MinIO ──────────────────
        logger.info("Reading cleansed claims from %s", input_path)
        cleansed_df = spark.read.parquet(input_path)

        raw_count = cleansed_df.count()
        logger.info("Cleansed record count: %s", raw_count)
        cleansed_df.printSchema()

        # ── Read Reference Data ──────────────────────────────
        logger.info("Loading reference data...")
        cpt_df = (
            spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .csv(CPT_REF_PATH)
            .select(
                col("cpt_code").cast("string").alias("cpt_code"),
                col("description").alias("cpt_description"),
                col("category").alias("cpt_category"),
                col("typical_duration_mins"),
            )
        )

        icd10_df = (
            spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .csv(ICD10_REF_PATH)
            .select(
                col("icd10_code").cast("string").alias("icd10_code"),
                col("description").alias("icd10_description"),
                col("category").alias("icd10_category"),
                col("chronic_flag"),
            )
        )

        logger.info("CPT codes loaded: %s",   cpt_df.count())
        logger.info("ICD10 codes loaded: %s", icd10_df.count())
        cpt_df.show(5, truncate=False)
        icd10_df.show(5, truncate=False)

        # ── Join with Reference Data ─────────────────────────
        logger.info("Joining with reference data...")
        enriched_df = (
            cleansed_df
            .withColumn("cpt_code",   col("cpt_code").cast("string"))
            .withColumn("icd10_code", col("icd10_code").cast("string"))
            .join(cpt_df,   on="cpt_code",   how="left")
            .join(icd10_df, on="icd10_code", how="left")
        )
        logger.info("Enriched record count: %s", enriched_df.count())
        enriched_df.show(5, truncate=False)

        # ── Standardize Columns ──────────────────────────────
        logger.info("Standardizing columns...")
        standardized_df = (
            enriched_df
            .withColumn("billed_amount", F.round(col("billed_amount"), 2))
            .withColumn(
                "place_of_service_desc",
                when(col("place_of_service") == 11, lit("Office"))
                .when(col("place_of_service") == 21, lit("Inpatient Hospital"))
                .when(col("place_of_service") == 22, lit("Outpatient Hospital"))
                .when(col("place_of_service") == 23, lit("Emergency Room"))
                .when(col("place_of_service") == 31, lit("Skilled Nursing Facility"))
                .otherwise(lit("Other")),
            )
            .withColumn("cpt_code",     col("cpt_code").cast("string"))
            .withColumn("service_date", col("service_date").cast("date"))
        )

        # ── Derived Fields ───────────────────────────────────
        logger.info("Calculating derived fields...")
        transformed_df = (
            standardized_df
            .withColumn(
                "billed_category",
                when(col("billed_amount") < 200, lit("LOW"))
                .when(col("billed_amount") < 500, lit("MEDIUM"))
                .otherwise(lit("HIGH")),
            )
            .withColumn(
                "claim_age_days",
                datediff(current_timestamp().cast("date"), col("service_date")),
            )
            .withColumn(
                "chronic_flag",
                when(col("chronic_flag") == "true", lit(True)).otherwise(lit(False)),
            )
            .withColumn("transformed_at", current_timestamp())
        )

        transformed_count = transformed_df.count()
        logger.info("Transformed record count: %s", transformed_count)
        transformed_df.show(5, truncate=False)

        # ── Write Transformed Data as Delta ──────────────────
        logger.info("Writing transformed data to %s", output_path)
        (
            transformed_df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(output_path)
        )
        logger.info("Transformed data written as Delta ✅")

        # ── Payer Summary ────────────────────────────────────
        logger.info("Building payer summary...")
        payer_summary_df = (
            transformed_df
            .groupBy("payer_id", "billed_category")
            .agg(
                F.count("claim_id").alias("total_claims"),
                F.round(F.sum("billed_amount"), 2).alias("total_billed"),
                F.round(F.avg("billed_amount"), 2).alias("avg_billed"),
                F.round(F.avg("claim_age_days"), 1).alias("avg_claim_age_days"),
                F.countDistinct("member_id_hash").alias("unique_members"),
                F.countDistinct("cpt_code").alias("unique_cpt_codes"),
            )
            .withColumn("summarized_at", current_timestamp())
        )

        # ── CPT Summary ──────────────────────────────────────
        logger.info("Building CPT code summary...")
        cpt_summary_df = (
            transformed_df
            .groupBy("cpt_code", "cpt_description", "cpt_category")
            .agg(
                F.count("claim_id").alias("total_claims"),
                F.round(F.sum("billed_amount"), 2).alias("total_billed"),
                F.round(F.avg("billed_amount"), 2).alias("avg_billed"),
                F.countDistinct("payer_id").alias("unique_payers"),
                F.countDistinct("member_id_hash").alias("unique_members"),
            )
            .withColumn("summarized_at", current_timestamp())
        )

        payer_count   = payer_summary_df.count()
        cpt_count     = cpt_summary_df.count()
        summary_count = payer_count + cpt_count
        logger.info("Payer summary rows: %s", payer_count)
        logger.info("CPT summary rows:   %s", cpt_count)
        payer_summary_df.show(truncate=False)
        cpt_summary_df.show(truncate=False)

        # ── Write Summaries as Delta ─────────────────────────
        logger.info("Writing payer summary to %s", payer_summary_path)
        (
            payer_summary_df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(payer_summary_path)
        )

        logger.info("Writing CPT summary to %s", cpt_summary_path)
        (
            cpt_summary_df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(cpt_summary_path)
        )
        logger.info("Summaries written as Delta ✅")

        elapsed = round(time.time() - started, 2)
        logger.info("Claims transform job complete in %ss ✅", elapsed)

        log_to_postgres(
            status="SUCCESS",
            run_time=run_time,
            records_read=raw_count,
            records_written=transformed_count,
        )

    except Exception as e:
        logger.exception("Claims transform job failed for file: %s", file_name)
        try:
            log_to_postgres(
                status="FAILED",
                run_time=run_time,
                records_read=raw_count,
                records_written=transformed_count,
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