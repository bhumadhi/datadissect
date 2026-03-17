from datetime import datetime, timezone
import logging

import psycopg2
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, current_timestamp, lit, when, datediff


# ── Logging Setup ───────────────────────────────────────────
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ── Constants ───────────────────────────────────────────────
INPUT_PATH      = "s3a://healthcare-cleansed/claims/"
OUTPUT_PATH     = "s3a://healthcare-transformed/claims/"
SUMMARY_PATH    = "s3a://healthcare-transformed/summaries/"
CPT_REF_PATH    = "s3a://healthcare-reference/cpt_codes.csv"
ICD10_REF_PATH  = "s3a://healthcare-reference/icd10_codes.csv"
JOB_NAME        = "claims_transform_job"
RUN_TIME        = datetime.now(timezone.utc)

POSTGRES_HOST     = "postgres"
POSTGRES_PORT     = 5432
POSTGRES_DB       = "pipeline_db"
POSTGRES_USER     = "pgadmin"
POSTGRES_PASSWORD = "pgpassword123"

# Initialize counts
raw_count       = 0
transformed_count = 0
summary_count   = 0

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

# ── Spark Session with Delta ────────────────────────────────
builder = (
    SparkSession.builder
    .appName("Claims-Transform-Job")
    .config("spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.connection.timeout", "600000")
    .config("spark.hadoop.fs.s3a.socket.timeout", "600000")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("WARN")
logger.info("Spark session with Delta created successfully")

try:
    # ── Read Cleansed Claims from MinIO ─────────────────────
    logger.info("Reading cleansed claims from %s", INPUT_PATH)

    cleansed_df = (
        spark.read
        .parquet(INPUT_PATH)
    )

    raw_count = cleansed_df.count()
    logger.info("Cleansed record count: %s", raw_count)
    cleansed_df.printSchema()

    # ── Read Reference Data ──────────────────────────────────
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
            col("typical_duration_mins")
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
            col("chronic_flag")
        )
    )

    logger.info("CPT codes loaded: %s", cpt_df.count())
    logger.info("ICD10 codes loaded: %s", icd10_df.count())
    cpt_df.show(5, truncate=False)
    icd10_df.show(5, truncate=False)

    # ── Join with Reference Data ─────────────────────────────
    logger.info("Joining with reference data...")

    # Cast cpt_code to string for consistent join
    enriched_df = (
        cleansed_df
        .withColumn("cpt_code", col("cpt_code").cast("string"))
        .withColumn("icd10_code", col("icd10_code").cast("string"))
    )

    # Join with CPT reference
    enriched_df = (
        enriched_df
        .join(cpt_df, on="cpt_code", how="left")
    )

    # Join with ICD10 reference
    enriched_df = (
        enriched_df
        .join(icd10_df, on="icd10_code", how="left")
    )

    logger.info("Enriched record count: %s", enriched_df.count())
    enriched_df.show(5, truncate=False)
    # ── Standardize Columns ──────────────────────────────────
    logger.info("Standardizing columns...")

    standardized_df = (
        enriched_df
        # Standardize billed_amount to 2 decimal places
        .withColumn("billed_amount",
            F.round(col("billed_amount"), 2))
        # Standardize place_of_service code to description
        .withColumn("place_of_service_desc",
            when(col("place_of_service") == 11, lit("Office"))
            .when(col("place_of_service") == 21, lit("Inpatient Hospital"))
            .when(col("place_of_service") == 22, lit("Outpatient Hospital"))
            .when(col("place_of_service") == 23, lit("Emergency Room"))
            .when(col("place_of_service") == 31, lit("Skilled Nursing Facility"))
            .otherwise(lit("Other")))
        # Ensure cpt_code is always string
        .withColumn("cpt_code",
            col("cpt_code").cast("string"))
        # Ensure service_date is date type
        .withColumn("service_date",
            col("service_date").cast("date"))
    )

    # ── Calculate Derived Fields ─────────────────────────────
    logger.info("Calculating derived fields...")

    transformed_df = (
        standardized_df
        # Billed amount category
        .withColumn("billed_category",
            when(col("billed_amount") < 200, lit("LOW"))
            .when(col("billed_amount") < 500, lit("MEDIUM"))
            .otherwise(lit("HIGH")))
        # Claim age in days from service date to processed date
        .withColumn("claim_age_days",
            datediff(
                current_timestamp().cast("date"),
                col("service_date")
            ))
        # Chronic condition flag from ICD10 reference
        .withColumn("chronic_flag",
            when(col("chronic_flag") == "true", lit(True))
            .otherwise(lit(False)))
        # Add transform timestamp
        .withColumn("transformed_at", current_timestamp())
    )

    transformed_count = transformed_df.count()
    logger.info("Transformed record count: %s", transformed_count)
    transformed_df.show(5, truncate=False)
    # ── Write Transformed Data as Delta ─────────────────────
    logger.info("Writing transformed data to %s", OUTPUT_PATH)

    (
        transformed_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(OUTPUT_PATH)
    )

    logger.info("Transformed data written as Delta ✅")

    # ── Aggregations — Payer Summary ─────────────────────────
    logger.info("Building payer summary...")

    payer_summary_df = (
        transformed_df
        .groupBy(
            "payer_id",
            "billed_category"
        )
        .agg(
            F.count("claim_id").alias("total_claims"),
            F.round(F.sum("billed_amount"), 2).alias("total_billed"),
            F.round(F.avg("billed_amount"), 2).alias("avg_billed"),
            F.round(F.avg("claim_age_days"), 1).alias("avg_claim_age_days"),
            F.countDistinct("member_id_hash").alias("unique_members"),
            F.countDistinct("cpt_code").alias("unique_cpt_codes")
        )
        .withColumn("summarized_at", current_timestamp())
    )

    # ── Aggregations — CPT Code Summary ─────────────────────
    logger.info("Building CPT code summary...")

    cpt_summary_df = (
        transformed_df
        .groupBy(
            "cpt_code",
            "cpt_description",
            "cpt_category"
        )
        .agg(
            F.count("claim_id").alias("total_claims"),
            F.round(F.sum("billed_amount"), 2).alias("total_billed"),
            F.round(F.avg("billed_amount"), 2).alias("avg_billed"),
            F.countDistinct("payer_id").alias("unique_payers"),
            F.countDistinct("member_id_hash").alias("unique_members")
        )
        .withColumn("summarized_at", current_timestamp())
    )

    payer_count = payer_summary_df.count()
    cpt_count = cpt_summary_df.count()
    summary_count = payer_count + cpt_count
    logger.info("Payer summary rows: %s", payer_count)
    logger.info("CPT summary rows: %s", cpt_count)

    payer_summary_df.show(truncate=False)
    cpt_summary_df.show(truncate=False)

    # ── Write Summaries as Delta ─────────────────────────────
    logger.info("Writing summaries to %s", SUMMARY_PATH)

    (
        payer_summary_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(SUMMARY_PATH + "payer/")
    )

    (
        cpt_summary_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(SUMMARY_PATH + "cpt/")
    )

    logger.info("Summaries written as Delta ✅")
    
    # ── Log success to PostgreSQL ────────────────────────────────────
    log_to_postgres(
        status="SUCCESS",
        records_read=raw_count,
        records_written=transformed_count,
        records_rejected=0,
        error_msg=None,
    )

    logger.info("Claims transform job complete! ✅")
except Exception as e:
    logger.exception("Claims transform job failed")
    try:
        log_to_postgres(
            status="FAILED",
            records_read=raw_count,
            records_written=transformed_count,
            records_rejected=0,
            error_msg=str(e),
        )
    except Exception:
        logger.exception("Failed to log failure to PostgreSQL")
    raise

finally:
    spark.stop()
    logger.info("Spark session stopped")