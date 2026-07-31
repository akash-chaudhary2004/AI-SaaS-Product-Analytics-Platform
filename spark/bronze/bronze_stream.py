"""
AI SaaS Product Analytics Platform
Bronze Streaming Layer

Purpose
-------
Read raw AI SaaS events from Kafka using Spark Structured Streaming
and persist them into the Bronze layer.

Pipeline
--------
AI SaaS Simulator
        ↓
Kafka Producer
        ↓
Kafka Topic: ai-product-events
        ↓
Spark Structured Streaming
        ↓
Bronze Parquet Storage

Bronze keeps the original Kafka event together with Kafka metadata.
No business-level cleaning or transformation happens here.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp


# ==========================================================
# Configuration
# ==========================================================

APP_NAME = "AI-SaaS-Bronze-Streaming"

KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"

KAFKA_TOPIC = "ai-product-events"

BRONZE_OUTPUT_PATH = "/app/data/bronze/events"

CHECKPOINT_PATH = "/app/data/checkpoints/bronze"


# ==========================================================
# Spark Session
# ==========================================================

def create_spark_session():
    """
    Create the Spark session used by the Bronze streaming job.
    """

    spark = (
        SparkSession.builder
        .appName(APP_NAME)
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark


# ==========================================================
# Kafka Source
# ==========================================================

def read_kafka_stream(spark):
    """
    Read streaming events from the Kafka topic.
    """

    kafka_stream = (
        spark.readStream
        .format("kafka")
        .option(
            "kafka.bootstrap.servers",
            KAFKA_BOOTSTRAP_SERVERS,
        )
        .option(
            "subscribe",
            KAFKA_TOPIC,
        )
        .option(
            "startingOffsets",
            "earliest",
        )
        .option(
            "failOnDataLoss",
            "false",
        )
        .load()
    )

    return kafka_stream


# ==========================================================
# Bronze Schema
# ==========================================================

def prepare_bronze_events(kafka_stream):
    """
    Prepare raw Kafka messages for Bronze storage.

    The JSON event remains unchanged in raw_event.

    Kafka metadata is stored so we can trace where every
    event came from.
    """

    bronze_events = kafka_stream.select(

        # Original event JSON
        col("value")
        .cast("string")
        .alias("raw_event"),

        # Kafka metadata
        col("topic").alias("kafka_topic"),

        col("partition").alias("kafka_partition"),

        col("offset").alias("kafka_offset"),

        col("timestamp").alias("kafka_timestamp"),

        # Time when Spark ingested the event
        current_timestamp().alias("ingested_at"),
    )

    return bronze_events


# ==========================================================
# Bronze Writer
# ==========================================================

def write_bronze_stream(bronze_events):
    """
    Persist Bronze events as Parquet files.

    Checkpointing allows Spark to remember which Kafka
    offsets have already been processed.
    """

    query = (
        bronze_events.writeStream

        .format("parquet")

        .outputMode("append")

        .option(
            "path",
            BRONZE_OUTPUT_PATH,
        )

        .option(
            "checkpointLocation",
            CHECKPOINT_PATH,
        )

        .trigger(
            processingTime="5 seconds"
        )

        .start()
    )

    return query


# ==========================================================
# Main Application
# ==========================================================

def main():
    """
    Start the Bronze Structured Streaming pipeline.
    """

    print("=" * 80)
    print("AI SaaS Bronze Streaming Pipeline")
    print("=" * 80)

    print(f"Kafka topic:      {KAFKA_TOPIC}")
    print(f"Kafka server:     {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Bronze output:    {BRONZE_OUTPUT_PATH}")
    print(f"Checkpoint path:  {CHECKPOINT_PATH}")

    print("=" * 80)

    spark = create_spark_session()

    print("Spark session created.")

    kafka_stream = read_kafka_stream(
        spark
    )

    print("Kafka streaming source created.")

    bronze_events = prepare_bronze_events(
        kafka_stream
    )

    print("Bronze event structure prepared.")

    query = write_bronze_stream(
        bronze_events
    )

    print("=" * 80)
    print("BRONZE STREAM STARTED")
    print("=" * 80)

    print("Waiting for Kafka events...")
    print("Press Ctrl+C to stop.")

    query.awaitTermination()


# ==========================================================
# Entry Point
# ==========================================================

if __name__ == "__main__":
    main()