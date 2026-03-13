from datetime import datetime, timezone
import logging

import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, current_timestamp, lit, sha2


# ── Logging Setup ───────────────────────────────────────────
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ── Constants ───────────────────────────────────────────────
INPUT_PATH = "s3a://healthcare-raw/BCBS001_837P_20260312.csv"
OUTPUT_PATH = "s3a://healthcare-cleansed/claims/"
QUARANTINE_PATH = "s3a://healthcare-quarantine/claims/"
SOURCE_FILE = "BCBS001_837P_20260312.csv"
JOB_NAME = "claims_cleanse_job"
RUN_TIME = datetime.now(timezone.utc)

POSTGRES_HOST = "postgres"
POSTGRES_PORT = 5432
POSTGRES_DB = "pipeline_db"
POSTGRES_USER = "pgadmin"
POSTGRES_PASSWORD = "pgpassword123"

# Initialize counts so failure logging never breaks
raw_count = 0
cleansed_count = 0
quarantine_count = 0


# ── Helper: PostgreSQL Logging ──────────────────────────────
def log_to_postgres(
    status: str,
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
                RUN_TIME,
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


# ── Spark Session ───────────────────────────────────────────
spark = (
    SparkSession.builder
    .appName("Claims-Cleanse-Job")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.connection.timeout", "600000")
    .config("spark.hadoop.fs.s3a.socket.timeout", "600000")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")
logger.info("Spark session created successfully")


try:
    # ── Read CSV from MinIO ─────────────────────────────────
    logger.info("Reading claims from %s", INPUT_PATH)

    raw_df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(INPUT_PATH)
    )

    raw_count = raw_df.count()
    logger.info("Raw record count: %s", raw_count)
    raw_df.printSchema()
    raw_df.show(5, truncate=False)

    # ── Validation Rules ────────────────────────────────────
    logger.info("Applying validation rules...")

    validated_df = (
        raw_df.withColumn(
            "is_valid",
            (col("claim_id").isNotNull())
            & (col("member_id").isNotNull())
            & (col("provider_npi").isNotNull())
            & (col("billed_amount").isNotNull())
            & (col("billed_amount") > 0)
        )
        .withColumn(
            "validation_errors",
            F.concat_ws(
                ", ",
                F.when(col("claim_id").isNull(), lit("missing claim_id")),
                F.when(col("member_id").isNull(), lit("missing member_id")),
                F.when(col("provider_npi").isNull(), lit("missing provider_npi")),
                F.when(col("billed_amount").isNull(), lit("missing billed_amount")),
                F.when(col("billed_amount") <= 0, lit("invalid billed_amount")),
            ),
        )
    )

    clean_df = (
        validated_df
        .filter(col("is_valid") == True)
        .drop("is_valid", "validation_errors")
    )

    quarantine_df = (
        validated_df
        .filter(col("is_valid") == False)
        .drop("is_valid")
    )

    quarantine_count = quarantine_df.count()
    logger.info("Quarantine records: %s", quarantine_count)

    # ── PHI Masking ─────────────────────────────────────────
    logger.info("Applying PHI masking...")

    cleansed_df = (
        clean_df
        .withColumn("member_id_hash", sha2(col("member_id"), 256))
        .withColumn("provider_npi_hash", sha2(col("provider_npi"), 256))
        .drop("member_id", "provider_npi")
        .withColumn("processed_at", current_timestamp())
        .withColumn("source_file", lit(SOURCE_FILE))
    )

    cleansed_count = cleansed_df.count()
    logger.info("Cleansed records: %s", cleansed_count)
    logger.info("PHI masking complete")
    cleansed_df.show(5, truncate=False)

    # ── Write Cleansed Data ─────────────────────────────────
    logger.info("Writing cleansed data to %s", OUTPUT_PATH)
    cleansed_df.write.mode("overwrite").parquet(OUTPUT_PATH)

    # ── Write Quarantine Data ───────────────────────────────
    if quarantine_count > 0:
        logger.info("Writing quarantine data to %s", QUARANTINE_PATH)
        quarantine_df.write.mode("overwrite").parquet(QUARANTINE_PATH)

    # ── Log Success ─────────────────────────────────────────
    log_to_postgres(
        status="SUCCESS",
        records_read=raw_count,
        records_written=cleansed_count,
        records_rejected=quarantine_count,
        error_msg=None,
    )

    logger.info("Claims cleanse job complete! ✅")

except Exception as e:
    logger.exception("Claims cleanse job failed")

    try:
        log_to_postgres(
            status="FAILED",
            records_read=0,
            records_written=0,
            records_rejected=0,
            error_msg=str(e),
        )
    except Exception:
        logger.exception("Failed to log job failure to PostgreSQL")

    raise

finally:
    spark.stop()
    logger.info("Spark session stopped")