from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit, sha2
from pyspark.sql import functions as F
from datetime import datetime, timezone
import logging

# ── Logging Setup ───────────────────────────────────────────
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ── Spark Session ───────────────────────────────────────────
spark = SparkSession.builder \
    .appName("Claims-Cleanse-Job") \
    .master("local[*]") \
    .config("spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
logger.info("Spark session created successfully")

# ── Constants ───────────────────────────────────────────────
INPUT_PATH = "s3a://healthcare-raw/BCBS001_837P_20260312.csv"
OUTPUT_PATH = "s3a://healthcare-cleansed/claims/"
QUARANTINE_PATH = "s3a://healthcare-quarantine/claims/"
JOB_NAME    = "claims_cleanse_job"
RUN_TIME = datetime.now(timezone.utc)

# ── Read CSV from MinIO ─────────────────────────────────────
logger.info(f"Reading claims from {INPUT_PATH}")

raw_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(INPUT_PATH)

logger.info(f"Raw record count: {raw_df.count()}")
raw_df.printSchema()
raw_df.show(5, truncate=False)

# ── Validation Rules ────────────────────────────────────────
logger.info("Applying validation rules...")

# Tag each record as valid or invalid
validated_df = raw_df.withColumn(
    "is_valid",
    (col("claim_id").isNotNull()) &
    (col("member_id").isNotNull()) &
    (col("provider_npi").isNotNull()) &
    (col("billed_amount").isNotNull()) &
    (col("billed_amount") > 0)
).withColumn(
    "validation_errors",
    F.concat_ws(", ",
        F.when(col("claim_id").isNull(), lit("missing claim_id")),
        F.when(col("member_id").isNull(), lit("missing member_id")),
        F.when(col("provider_npi").isNull(), lit("missing provider_npi")),
        F.when(col("billed_amount").isNull(), lit("missing billed_amount")),
        F.when(col("billed_amount") <= 0, lit("invalid billed_amount"))
    )
)

# ── Split Clean vs Quarantine ───────────────────────────────
clean_df = validated_df.filter(col("is_valid") == True) \
    .drop("is_valid", "validation_errors")

quarantine_df = validated_df.filter(col("is_valid") == False) \
    .drop("is_valid")

logger.info(f"Clean records:      {clean_df.count()}")
logger.info(f"Quarantine records: {quarantine_df.count()}")

# ── PHI Masking ─────────────────────────────────────────────
logger.info("Applying PHI masking...")

cleansed_df = clean_df \
    .withColumn("member_id_hash",
        sha2(col("member_id"), 256)) \
    .withColumn("provider_npi_hash",
        sha2(col("provider_npi"), 256)) \
    .drop("member_id", "provider_npi") \
    .withColumn("processed_at", current_timestamp()) \
    .withColumn("source_file",
        lit("BCBS001_837P_20260312.csv"))

logger.info("PHI masking complete")
cleansed_df.show(5, truncate=False)


# ── Write Cleansed Data to MinIO ────────────────────────────
logger.info(f"Writing cleansed data to {OUTPUT_PATH}")

cleansed_df.write \
    .mode("overwrite") \
    .parquet(OUTPUT_PATH)

# ── Write Quarantine Data to MinIO ──────────────────────────
if quarantine_df.count() > 0:
    logger.info(f"Writing quarantine data to {QUARANTINE_PATH}")
    quarantine_df.write \
        .mode("overwrite") \
        .parquet(QUARANTINE_PATH)

# ── Log to PostgreSQL ────────────────────────────────────────
logger.info("Logging pipeline run to PostgreSQL...")

import psycopg2

conn = psycopg2.connect(
    host="localhost",
    port=5432,
    database="pipeline_db",
    user="pgadmin",
    password="pgpassword123"
)

cursor = conn.cursor()
cursor.execute("""
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
""", (
    JOB_NAME,
    "SUCCESS",
    RUN_TIME,
    datetime.now(timezone.utc),
    raw_df.count(),
    cleansed_df.count(),
    quarantine_df.count(),
    None
))

conn.commit()
cursor.close()
conn.close()

logger.info("Pipeline run logged successfully!")
logger.info("Claims cleanse job complete! ✅")

# ── Stop Spark ───────────────────────────────────────────────
spark.stop()