"""
AI SaaS Product Analytics Platform
Silver Processing Layer

Purpose
-------
Read raw Bronze Parquet events, parse the JSON payload,
apply a defined schema, validate important fields,
remove duplicate events, and write clean structured
records into the Silver layer.

Pipeline
--------
Kafka
  ↓
Spark Structured Streaming
  ↓
Bronze Parquet
  ↓
Silver Processing
  ├── Parse JSON
  ├── Apply Schema
  ├── Validate Records
  ├── Correct Data Types
  └── Remove Duplicates
  ↓
Silver Parquet
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp,
)
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)


# ==========================================================
# Configuration
# ==========================================================

APP_NAME = "AI-SaaS-Silver-Processing"

BRONZE_INPUT_PATH = "/app/data/bronze/events"

SILVER_OUTPUT_PATH = "/app/data/silver/events"


# ==========================================================
# Event Schema
# ==========================================================

EVENT_SCHEMA = StructType(
    [
        # --------------------------------------------------
        # Common event fields
        # --------------------------------------------------

        StructField("event_id", StringType(), True),

        StructField("timestamp", StringType(), True),

        StructField("event_type", StringType(), True),

        StructField("user_id", StringType(), True),

        StructField("session_id", StringType(), True),

        StructField("country", StringType(), True),

        StructField("subscription_plan", StringType(), True),

        StructField("model_name", StringType(), True),

        # --------------------------------------------------
        # Login fields
        # --------------------------------------------------

        StructField("browser", StringType(), True),

        StructField("device_type", StringType(), True),

        StructField("operating_system", StringType(), True),

        StructField("ip_address", StringType(), True),

        # --------------------------------------------------
        # Chat fields
        # --------------------------------------------------

        StructField("chat_id", StringType(), True),

        # --------------------------------------------------
        # Prompt fields
        # --------------------------------------------------

        StructField("prompt_category", StringType(), True),

        StructField("prompt_text", StringType(), True),

        # --------------------------------------------------
        # Response fields
        # --------------------------------------------------

        StructField("latency_ms", IntegerType(), True),

        StructField("prompt_tokens", IntegerType(), True),

        StructField("completion_tokens", IntegerType(), True),

        StructField("total_tokens", IntegerType(), True),

        StructField("response_cost", DoubleType(), True),

        StructField("success", BooleanType(), True),

        # --------------------------------------------------
        # Feedback fields
        # --------------------------------------------------

        StructField("feedback", StringType(), True),

        StructField("rating", IntegerType(), True),

        # --------------------------------------------------
        # Logout fields
        # --------------------------------------------------

        StructField(
            "session_duration_minutes",
            IntegerType(),
            True,
        ),
    ]
)


# ==========================================================
# Spark Session
# ==========================================================

def create_spark_session():
    """
    Create the Spark session for Silver processing.
    """

    spark = (
        SparkSession.builder
        .appName(APP_NAME)
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark


# ==========================================================
# Read Bronze
# ==========================================================

def read_bronze_data(spark):
    """
    Read raw Bronze Parquet files.
    """

    bronze_df = spark.read.parquet(
        BRONZE_INPUT_PATH
    )

    return bronze_df


# ==========================================================
# Parse Raw JSON
# ==========================================================

def parse_events(bronze_df):
    """
    Parse raw_event JSON using the explicit event schema.
    """

    parsed_df = bronze_df.withColumn(
        "event",
        from_json(
            col("raw_event"),
            EVENT_SCHEMA,
        ),
    )

    return parsed_df


# ==========================================================
# Flatten Event Data
# ==========================================================

def flatten_events(parsed_df):
    """
    Convert the parsed event structure into normal columns
    while preserving important Kafka metadata.
    """

    silver_df = parsed_df.select(

        # Common fields
        col("event.event_id").alias("event_id"),

        to_timestamp(
            col("event.timestamp")
        ).alias("event_timestamp"),

        col("event.event_type").alias("event_type"),

        col("event.user_id").alias("user_id"),

        col("event.session_id").alias("session_id"),

        col("event.country").alias("country"),

        col("event.subscription_plan").alias(
            "subscription_plan"
        ),

        col("event.model_name").alias("model_name"),

        # Login fields
        col("event.browser").alias("browser"),

        col("event.device_type").alias("device_type"),

        col("event.operating_system").alias(
            "operating_system"
        ),

        col("event.ip_address").alias("ip_address"),

        # Chat fields
        col("event.chat_id").alias("chat_id"),

        # Prompt fields
        col("event.prompt_category").alias(
            "prompt_category"
        ),

        col("event.prompt_text").alias("prompt_text"),

        # Response fields
        col("event.latency_ms").alias("latency_ms"),

        col("event.prompt_tokens").alias(
            "prompt_tokens"
        ),

        col("event.completion_tokens").alias(
            "completion_tokens"
        ),

        col("event.total_tokens").alias(
            "total_tokens"
        ),

        col("event.response_cost").alias(
            "response_cost"
        ),

        col("event.success").alias("success"),

        # Feedback fields
        col("event.feedback").alias("feedback"),

        col("event.rating").alias("rating"),

        # Logout fields
        col(
            "event.session_duration_minutes"
        ).alias("session_duration_minutes"),

        # Kafka metadata retained from Bronze
        col("kafka_topic"),

        col("kafka_partition"),

        col("kafka_offset"),

        col("kafka_timestamp"),

        col("ingested_at"),
    )

    return silver_df


# ==========================================================
# Data Validation
# ==========================================================

def validate_events(silver_df):
    """
    Remove unusable records.

    Every valid event must contain:
    - event_id
    - event_type
    - user_id
    - session_id
    - event timestamp
    """

    valid_df = silver_df.filter(
        col("event_id").isNotNull()
        & col("event_type").isNotNull()
        & col("user_id").isNotNull()
        & col("session_id").isNotNull()
        & col("event_timestamp").isNotNull()
    )

    return valid_df


# ==========================================================
# Remove Duplicates
# ==========================================================

def remove_duplicates(silver_df):
    """
    Remove duplicate events using event_id.

    event_id is unique for every generated SaaS event.
    """

    deduplicated_df = silver_df.dropDuplicates(
        ["event_id"]
    )

    return deduplicated_df


# ==========================================================
# Write Silver
# ==========================================================

def write_silver_data(silver_df):
    """
    Write clean structured events to Silver Parquet.
    """

    (
        silver_df.write
        .mode("overwrite")
        .parquet(SILVER_OUTPUT_PATH)
    )


# ==========================================================
# Main Application
# ==========================================================

def main():
    """
    Execute the complete Bronze → Silver transformation.
    """

    print("=" * 80)
    print("AI SaaS Silver Processing Layer")
    print("=" * 80)

    print(f"Bronze input:  {BRONZE_INPUT_PATH}")
    print(f"Silver output: {SILVER_OUTPUT_PATH}")

    print("=" * 80)

    spark = create_spark_session()

    print("Spark session created.")

    # ------------------------------------------------------
    # Read Bronze
    # ------------------------------------------------------

    bronze_df = read_bronze_data(
        spark
    )

    bronze_count = bronze_df.count()

    print(
        f"Bronze records read: {bronze_count}"
    )

    # ------------------------------------------------------
    # Parse JSON
    # ------------------------------------------------------

    parsed_df = parse_events(
        bronze_df
    )

    # ------------------------------------------------------
    # Flatten structure
    # ------------------------------------------------------

    silver_df = flatten_events(
        parsed_df
    )

    # ------------------------------------------------------
    # Validate
    # ------------------------------------------------------

    valid_df = validate_events(
        silver_df
    )

    valid_count = valid_df.count()

    print(
        f"Valid records: {valid_count}"
    )

    invalid_count = (
        bronze_count - valid_count
    )

    print(
        f"Invalid records removed: {invalid_count}"
    )

    # ------------------------------------------------------
    # Deduplicate
    # ------------------------------------------------------

    clean_df = remove_duplicates(
        valid_df
    )

    clean_count = clean_df.count()

    duplicate_count = (
        valid_count - clean_count
    )

    print(
        f"Duplicate records removed: {duplicate_count}"
    )

    print(
        f"Final Silver records: {clean_count}"
    )

    # ------------------------------------------------------
    # Write Silver
    # ------------------------------------------------------

    write_silver_data(
        clean_df
    )

    print("=" * 80)
    print("SILVER PROCESSING COMPLETED")
    print("=" * 80)

    print(
        f"{clean_count} clean events written to:"
    )

    print(
        SILVER_OUTPUT_PATH
    )

    print("=" * 80)

    spark.stop()


# ==========================================================
# Entry Point
# ==========================================================

if __name__ == "__main__":
    main()