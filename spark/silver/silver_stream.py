"""
AI SaaS Product Analytics Platform
Silver Processing Layer

Purpose
-------
Read raw Bronze events, parse JSON payloads, normalize the event schema,
apply event-aware validation, preserve business fields, remove duplicates,
and write analytics-ready Silver Parquet data.

Pipeline
--------
Kafka
    ↓
Bronze
    ↓
Silver
    ├── JSON parsing
    ├── Schema enforcement
    ├── Event-aware validation
    ├── Type conversion
    ├── Event ID generation
    ├── Duplicate removal
    ├── Data-quality reporting
    └── Parquet output
"""

from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


# ==============================================================================
# Configuration
# ==============================================================================

APP_NAME = "AI-SaaS-Silver-Processing"

BRONZE_INPUT_PATH = "/app/data/bronze/events"

SILVER_OUTPUT_PATH = "/app/data/silver/events"


# ==============================================================================
# Event Schema
# ==============================================================================

EVENT_SCHEMA = StructType(
    [

        # ----------------------------------------------------------------------
        # Common Event Information
        # ----------------------------------------------------------------------

        StructField(
            "event_id",
            StringType(),
            True,
        ),

        StructField(
            "timestamp",
            StringType(),
            True,
        ),

        StructField(
            "event_type",
            StringType(),
            True,
        ),

        StructField(
            "user_id",
            StringType(),
            True,
        ),

        StructField(
            "session_id",
            StringType(),
            True,
        ),

        # ----------------------------------------------------------------------
        # User / Customer Information
        # ----------------------------------------------------------------------

        StructField(
            "signup_timestamp",
            StringType(),
            True,
        ),

        StructField(
            "country",
            StringType(),
            True,
        ),

        StructField(
            "region",
            StringType(),
            True,
        ),

        StructField(
            "city",
            StringType(),
            True,
        ),

        StructField(
            "timezone",
            StringType(),
            True,
        ),

        StructField(
            "language",
            StringType(),
            True,
        ),

        StructField(
            "currency",
            StringType(),
            True,
        ),

        StructField(
            "industry",
            StringType(),
            True,
        ),

        StructField(
            "company_size",
            StringType(),
            True,
        ),

        StructField(
            "job_role",
            StringType(),
            True,
        ),

        StructField(
            "device_preference",
            StringType(),
            True,
        ),

        # ----------------------------------------------------------------------
        # Subscription Information
        # ----------------------------------------------------------------------

        StructField(
            "subscription_plan",
            StringType(),
            True,
        ),

        StructField(
            "plan_price",
            DoubleType(),
            True,
        ),

        StructField(
            "subscription_id",
            StringType(),
            True,
        ),

        StructField(
            "monthly_price",
            DoubleType(),
            True,
        ),

        StructField(
            "billing_cycle",
            StringType(),
            True,
        ),

        StructField(
            "trial_start",
            StringType(),
            True,
        ),

        StructField(
            "trial_end",
            StringType(),
            True,
        ),

        StructField(
            "subscription_status",
            StringType(),
            True,
        ),

        # ----------------------------------------------------------------------
        # Payment Information
        # ----------------------------------------------------------------------

        StructField(
            "invoice_id",
            StringType(),
            True,
        ),

        StructField(
            "payment_id",
            StringType(),
            True,
        ),

        StructField(
            "amount",
            DoubleType(),
            True,
        ),

        StructField(
            "payment_status",
            StringType(),
            True,
        ),

        # ----------------------------------------------------------------------
        # Session / Device Information
        # ----------------------------------------------------------------------

        StructField(
            "login_time",
            StringType(),
            True,
        ),

        StructField(
            "logout_time",
            StringType(),
            True,
        ),

        StructField(
            "session_duration_minutes",
            IntegerType(),
            True,
        ),

        StructField(
            "browser",
            StringType(),
            True,
        ),

        StructField(
            "device_type",
            StringType(),
            True,
        ),

        StructField(
            "operating_system",
            StringType(),
            True,
        ),

        StructField(
            "ip_address",
            StringType(),
            True,
        ),

        # ----------------------------------------------------------------------
        # Chat Information
        # ----------------------------------------------------------------------

        StructField(
            "chat_id",
            StringType(),
            True,
        ),

        # ----------------------------------------------------------------------
        # Prompt Information
        # ----------------------------------------------------------------------

        StructField(
            "prompt_id",
            StringType(),
            True,
        ),

        StructField(
            "prompt_category",
            StringType(),
            True,
        ),

        StructField(
            "prompt_text",
            StringType(),
            True,
        ),

        StructField(
            "prompt_language",
            StringType(),
            True,
        ),

        StructField(
            "prompt_length",
            IntegerType(),
            True,
        ),

        StructField(
            "prompt_complexity",
            StringType(),
            True,
        ),

        StructField(
            "contains_code",
            BooleanType(),
            True,
        ),

        StructField(
            "contains_image",
            BooleanType(),
            True,
        ),

        # ----------------------------------------------------------------------
        # AI Model Information
        # ----------------------------------------------------------------------

        StructField(
            "model_provider",
            StringType(),
            True,
        ),

        StructField(
            "model_name",
            StringType(),
            True,
        ),

        # ----------------------------------------------------------------------
        # AI Performance Information
        # ----------------------------------------------------------------------

        StructField(
            "latency_ms",
            IntegerType(),
            True,
        ),

        StructField(
            "queue_time_ms",
            IntegerType(),
            True,
        ),

        StructField(
            "inference_time_ms",
            IntegerType(),
            True,
        ),

        StructField(
            "render_time_ms",
            IntegerType(),
            True,
        ),

        StructField(
            "prompt_tokens",
            IntegerType(),
            True,
        ),

        StructField(
            "completion_tokens",
            IntegerType(),
            True,
        ),

        StructField(
            "total_tokens",
            IntegerType(),
            True,
        ),

        StructField(
            "response_cost",
            DoubleType(),
            True,
        ),

        StructField(
            "success",
            BooleanType(),
            True,
        ),

        StructField(
            "retry_count",
            IntegerType(),
            True,
        ),

        StructField(
            "cache_hit",
            BooleanType(),
            True,
        ),

        StructField(
            "finish_reason",
            StringType(),
            True,
        ),

        StructField(
            "temperature",
            DoubleType(),
            True,
        ),

        StructField(
            "max_tokens",
            IntegerType(),
            True,
        ),

        # ----------------------------------------------------------------------
        # Feedback Information
        # ----------------------------------------------------------------------

        StructField(
            "feedback",
            StringType(),
            True,
        ),

        StructField(
            "rating",
            IntegerType(),
            True,
        ),

        StructField(
            "feedback_reason",
            StringType(),
            True,
        ),
    ]
)


# ==============================================================================
# Spark Session
# ==============================================================================

def create_spark_session() -> SparkSession:
    """
    Create Spark session for Silver processing.
    """

    spark = (
        SparkSession.builder
        .appName(APP_NAME)
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark


# ==============================================================================
# Read Bronze
# ==============================================================================

def read_bronze_data(
    spark: SparkSession,
) -> DataFrame:
    """
    Read Bronze Parquet dataset.
    """

    print()
    print("=" * 80)
    print("READING BRONZE DATASET")
    print("=" * 80)

    print(
        f"Bronze Input : {BRONZE_INPUT_PATH}"
    )

    bronze_df = spark.read.parquet(
        BRONZE_INPUT_PATH
    )

    print(
        f"Bronze Records : {bronze_df.count()}"
    )

    return bronze_df


# ==============================================================================
# Parse JSON
# ==============================================================================

def parse_events(
    bronze_df: DataFrame,
) -> DataFrame:
    """
    Parse raw JSON events using the explicit schema.
    """

    print()
    print("=" * 80)
    print("PARSING BRONZE JSON EVENTS")
    print("=" * 80)

    return (
        bronze_df
        .withColumn(
            "event",
            F.from_json(
                F.col("raw_event"),
                EVENT_SCHEMA,
            ),
        )
        .withColumn(
            "json_parse_success",
            F.col("event").isNotNull(),
        )
    )


# ==============================================================================
# Flatten Event Structure
# ==============================================================================

def flatten_events(
    parsed_df: DataFrame,
) -> DataFrame:
    """
    Convert nested event structure into flat analytics columns.
    """

    print()
    print("=" * 80)
    print("FLATTENING EVENT STRUCTURE")
    print("=" * 80)

    silver_df = parsed_df.select(

        # ----------------------------------------------------------------------
        # Event Identity
        # ----------------------------------------------------------------------

        F.col("event.event_id").alias(
            "event_id"
        ),

        F.to_timestamp(
            F.col("event.timestamp")
        ).alias(
            "event_timestamp"
        ),

        F.col("event.event_type").alias(
            "event_type"
        ),

        F.col("event.user_id").alias(
            "user_id"
        ),

        F.col("event.session_id").alias(
            "session_id"
        ),

        # ----------------------------------------------------------------------
        # User / Customer
        # ----------------------------------------------------------------------

        F.to_timestamp(
            F.col("event.signup_timestamp")
        ).alias(
            "signup_timestamp"
        ),

        F.col("event.country").alias(
            "country"
        ),

        F.col("event.region").alias(
            "region"
        ),

        F.col("event.city").alias(
            "city"
        ),

        F.col("event.timezone").alias(
            "timezone"
        ),

        F.col("event.language").alias(
            "language"
        ),

        F.col("event.currency").alias(
            "currency"
        ),

        F.col("event.industry").alias(
            "industry"
        ),

        F.col("event.company_size").alias(
            "company_size"
        ),

        F.col("event.job_role").alias(
            "job_role"
        ),

        F.col("event.device_preference").alias(
            "device_preference"
        ),

        # ----------------------------------------------------------------------
        # Subscription
        # ----------------------------------------------------------------------

        F.col("event.subscription_plan").alias(
            "subscription_plan"
        ),

        F.col("event.plan_price").alias(
            "plan_price"
        ),

        F.col("event.subscription_id").alias(
            "subscription_id"
        ),

        F.col("event.monthly_price").alias(
            "monthly_price"
        ),

        F.col("event.billing_cycle").alias(
            "billing_cycle"
        ),

        F.to_timestamp(
            F.col("event.trial_start")
        ).alias(
            "trial_start"
        ),

        F.to_timestamp(
            F.col("event.trial_end")
        ).alias(
            "trial_end"
        ),

        F.col("event.subscription_status").alias(
            "subscription_status"
        ),

        # ----------------------------------------------------------------------
        # Payment
        # ----------------------------------------------------------------------

        F.col("event.invoice_id").alias(
            "invoice_id"
        ),

        F.col("event.payment_id").alias(
            "payment_id"
        ),

        F.col("event.amount").alias(
            "amount"
        ),

        F.col("event.payment_status").alias(
            "payment_status"
        ),

        # ----------------------------------------------------------------------
        # Session / Device
        # ----------------------------------------------------------------------

        F.to_timestamp(
            F.col("event.login_time")
        ).alias(
            "login_time"
        ),

        F.to_timestamp(
            F.col("event.logout_time")
        ).alias(
            "logout_time"
        ),

        F.col(
            "event.session_duration_minutes"
        ).alias(
            "session_duration_minutes"
        ),

        F.col("event.browser").alias(
            "browser"
        ),

        F.col("event.device_type").alias(
            "device_type"
        ),

        F.col("event.operating_system").alias(
            "operating_system"
        ),

        F.col("event.ip_address").alias(
            "ip_address"
        ),

        # ----------------------------------------------------------------------
        # Chat
        # ----------------------------------------------------------------------

        F.col("event.chat_id").alias(
            "chat_id"
        ),

        # ----------------------------------------------------------------------
        # Prompt
        # ----------------------------------------------------------------------

        F.col("event.prompt_id").alias(
            "prompt_id"
        ),

        F.col("event.prompt_category").alias(
            "prompt_category"
        ),

        F.col("event.prompt_text").alias(
            "prompt_text"
        ),

        F.col("event.prompt_language").alias(
            "prompt_language"
        ),

        F.col("event.prompt_length").alias(
            "prompt_length"
        ),

        F.col("event.prompt_complexity").alias(
            "prompt_complexity"
        ),

        F.col("event.contains_code").alias(
            "contains_code"
        ),

        F.col("event.contains_image").alias(
            "contains_image"
        ),

        # ----------------------------------------------------------------------
        # Model
        # ----------------------------------------------------------------------

        F.col("event.model_provider").alias(
            "model_provider"
        ),

        F.col("event.model_name").alias(
            "model_name"
        ),

        # ----------------------------------------------------------------------
        # AI Performance
        # ----------------------------------------------------------------------

        F.col("event.latency_ms").alias(
            "latency_ms"
        ),

        F.col("event.queue_time_ms").alias(
            "queue_time_ms"
        ),

        F.col("event.inference_time_ms").alias(
            "inference_time_ms"
        ),

        F.col("event.render_time_ms").alias(
            "render_time_ms"
        ),

        F.col("event.prompt_tokens").alias(
            "prompt_tokens"
        ),

        F.col("event.completion_tokens").alias(
            "completion_tokens"
        ),

        F.col("event.total_tokens").alias(
            "total_tokens"
        ),

        F.col("event.response_cost").alias(
            "response_cost"
        ),

        F.col("event.success").alias(
            "success"
        ),

        F.col("event.retry_count").alias(
            "retry_count"
        ),

        F.col("event.cache_hit").alias(
            "cache_hit"
        ),

        F.col("event.finish_reason").alias(
            "finish_reason"
        ),

        F.col("event.temperature").alias(
            "temperature"
        ),

        F.col("event.max_tokens").alias(
            "max_tokens"
        ),

        # ----------------------------------------------------------------------
        # Feedback
        # ----------------------------------------------------------------------

        F.col("event.feedback").alias(
            "feedback"
        ),

        F.col("event.rating").alias(
            "rating"
        ),

        F.col("event.feedback_reason").alias(
            "feedback_reason"
        ),

        # ----------------------------------------------------------------------
        # Kafka Lineage
        # ----------------------------------------------------------------------

        F.col("kafka_topic").alias(
            "kafka_topic"
        ),

        F.col("kafka_partition").alias(
            "kafka_partition"
        ),

        F.col("kafka_offset").alias(
            "kafka_offset"
        ),

        F.col("kafka_timestamp").alias(
            "kafka_timestamp"
        ),

        F.col("ingested_at").alias(
            "ingested_at"
        ),

        # ----------------------------------------------------------------------
        # Parsing Status
        # ----------------------------------------------------------------------

        F.col("json_parse_success").alias(
            "json_parse_success"
        ),
    )

    return silver_df


# ==============================================================================
# Generate Missing Event IDs
# ==============================================================================

def generate_event_ids(
    silver_df: DataFrame,
) -> DataFrame:
    """
    Generate deterministic fallback event IDs for events where the
    producer did not provide event_id.

    Kafka topic + partition + offset uniquely identifies a Kafka record.
    """

    print()
    print("=" * 80)
    print("GENERATING MISSING EVENT IDS")
    print("=" * 80)

    missing_before = (
        silver_df
        .filter(
            F.col("event_id").isNull()
        )
        .count()
    )

    print(
        f"Missing event IDs : {missing_before}"
    )

    result_df = (
        silver_df
        .withColumn(
            "event_id",
            F.when(
                F.col("event_id").isNotNull(),
                F.col("event_id"),
            ).otherwise(
                F.concat(
                    F.lit("evt_kafka_"),
                    F.col("kafka_partition").cast("string"),
                    F.lit("_"),
                    F.col("kafka_offset").cast("string"),
                )
            ),
        )
    )

    missing_after = (
        result_df
        .filter(
            F.col("event_id").isNull()
        )
        .count()
    )

    print(
        f"Missing event IDs after fix : {missing_after}"
    )

    return result_df


# ==============================================================================
# Event-Aware Validation
# ==============================================================================

def validate_events(
    silver_df: DataFrame,
) -> DataFrame:
    """
    Validate events using rules appropriate to the event type.

    Required for every event:
        event_id
        event_type
        user_id
        event_timestamp

    Required for interaction/session events:
        session_id

    user_created, subscription_created and payment events
    do not require session_id.
    """

    print()
    print("=" * 80)
    print("VALIDATING SILVER EVENTS")
    print("=" * 80)

    session_events = [
        "session_started",
        "user_login",
        "chat_started",
        "prompt_submitted",
        "response_generated",
        "feedback_submitted",
        "user_logout",
    ]

    base_condition = (
        F.col("event_id").isNotNull()
        & F.col("event_type").isNotNull()
        & F.col("user_id").isNotNull()
        & F.col("event_timestamp").isNotNull()
        & F.col("json_parse_success")
    )

    session_condition = (
        ~F.col("event_type").isin(
            session_events
        )
        |
        F.col("session_id").isNotNull()
    )

    validated_df = (
        silver_df
        .withColumn(
            "validation_status",
            F.when(
                base_condition
                & session_condition,
                F.lit("valid"),
            ).otherwise(
                F.lit("invalid")
            ),
        )
    )

    valid_count = (
        validated_df
        .filter(
            F.col("validation_status") == "valid"
        )
        .count()
    )

    invalid_count = (
        validated_df
        .filter(
            F.col("validation_status") == "invalid"
        )
        .count()
    )

    print(
        f"Valid records   : {valid_count}"
    )

    print(
        f"Invalid records : {invalid_count}"
    )

    return (
        validated_df
        .filter(
            F.col("validation_status") == "valid"
        )
        .drop("validation_status")
    )


# ==============================================================================
# Remove Duplicates
# ==============================================================================

def remove_duplicates(
    silver_df: DataFrame,
) -> DataFrame:
    """
    Remove duplicate events using event_id.
    """

    print()
    print("=" * 80)
    print("REMOVING DUPLICATE EVENTS")
    print("=" * 80)

    before_count = silver_df.count()

    clean_df = (
        silver_df
        .dropDuplicates(
            ["event_id"]
        )
    )

    after_count = clean_df.count()

    print(
        f"Records before deduplication : {before_count}"
    )

    print(
        f"Duplicate records removed     : "
        f"{before_count - after_count}"
    )

    print(
        f"Records after deduplication   : {after_count}"
    )

    return clean_df


# ==============================================================================
# Data Quality Report
# ==============================================================================

def print_quality_report(
    silver_df: DataFrame,
) -> None:
    """
    Print Silver data-quality and business summary.
    """

    print()
    print("=" * 80)
    print("SILVER DATA QUALITY REPORT")
    print("=" * 80)

    total_records = silver_df.count()

    print(
        f"Total Silver Records : {total_records}"
    )

    print()
    print("Event Distribution")
    print("-" * 80)

    (
        silver_df
        .groupBy("event_type")
        .count()
        .orderBy(
            F.col("count").desc()
        )
        .show(
            50,
            truncate=False,
        )
    )

    print()
    print("Distinct Users")

    print(
        silver_df
        .select("user_id")
        .distinct()
        .count()
    )

    print()
    print("Distinct Sessions")

    print(
        silver_df
        .filter(
            F.col("session_id").isNotNull()
        )
        .select("session_id")
        .distinct()
        .count()
    )

    print()
    print("Distinct Subscriptions")

    print(
        silver_df
        .filter(
            F.col("subscription_id").isNotNull()
        )
        .select("subscription_id")
        .distinct()
        .count()
    )

    print()
    print("Successful Payments")

    print(
        silver_df
        .filter(
            F.col("payment_status") == "success"
        )
        .count()
    )

    print()
    print("Date Range")

    (
        silver_df
        .select(
            F.min(
                "event_timestamp"
            ).alias(
                "min_event_timestamp"
            ),
            F.max(
                "event_timestamp"
            ).alias(
                "max_event_timestamp"
            ),
        )
        .show(
            truncate=False
        )
    )


# ==============================================================================
# Write Silver
# ==============================================================================

def write_silver_data(
    silver_df: DataFrame,
) -> None:
    """
    Write Silver dataset as Parquet.
    """

    print()
    print("=" * 80)
    print("WRITING SILVER DATASET")
    print("=" * 80)

    (
        silver_df.write
        .mode("overwrite")
        .parquet(
            SILVER_OUTPUT_PATH
        )
    )

    print(
        f"Silver Output : {SILVER_OUTPUT_PATH}"
    )


# ==============================================================================
# Main
# ==============================================================================

def main():
    """
    Execute complete Bronze → Silver transformation.
    """

    print()
    print("=" * 80)
    print("AI SaaS SILVER PROCESSING LAYER")
    print("=" * 80)

    spark = None

    try:

        # ----------------------------------------------------------------------
        # Spark
        # ----------------------------------------------------------------------

        spark = create_spark_session()

        print(
            "Spark session created successfully."
        )

        # ----------------------------------------------------------------------
        # Bronze
        # ----------------------------------------------------------------------

        bronze_df = read_bronze_data(
            spark
        )

        bronze_count = bronze_df.count()

        # ----------------------------------------------------------------------
        # JSON Parsing
        # ----------------------------------------------------------------------

        parsed_df = parse_events(
            bronze_df
        )

        # ----------------------------------------------------------------------
        # Flatten
        # ----------------------------------------------------------------------

        silver_df = flatten_events(
            parsed_df
        )

        # ----------------------------------------------------------------------
        # Generate Missing IDs
        # ----------------------------------------------------------------------

        silver_df = generate_event_ids(
            silver_df
        )

        # ----------------------------------------------------------------------
        # Validate
        # ----------------------------------------------------------------------

        valid_df = validate_events(
            silver_df
        )

        valid_count = valid_df.count()

        # ----------------------------------------------------------------------
        # Deduplicate
        # ----------------------------------------------------------------------

        clean_df = remove_duplicates(
            valid_df
        )

        clean_count = clean_df.count()

        # ----------------------------------------------------------------------
        # Quality Report
        # ----------------------------------------------------------------------

        print_quality_report(
            clean_df
        )

        # ----------------------------------------------------------------------
        # Write
        # ----------------------------------------------------------------------

        write_silver_data(
            clean_df
        )

        # ----------------------------------------------------------------------
        # Final Summary
        # ----------------------------------------------------------------------

        print()
        print("=" * 80)
        print("SILVER PROCESSING COMPLETED SUCCESSFULLY")
        print("=" * 80)

        print(
            f"Bronze Records       : {bronze_count}"
        )

        print(
            f"Valid Records        : {valid_count}"
        )

        print(
            f"Final Silver Records : {clean_count}"
        )

        print(
            f"Records Removed      : "
            f"{bronze_count - clean_count}"
        )

        print(
            f"Silver Output        : "
            f"{SILVER_OUTPUT_PATH}"
        )

        print("=" * 80)

    except Exception as error:

        print()
        print("=" * 80)
        print("ERROR: SILVER PROCESSING FAILED")
        print("=" * 80)

        print(
            f"{type(error).__name__}: {error}"
        )

        raise

    finally:

        if spark is not None:

            spark.stop()

            print(
                "Spark session stopped."
            )


# ==============================================================================
# Entry Point
# ==============================================================================

if __name__ == "__main__":
    main()