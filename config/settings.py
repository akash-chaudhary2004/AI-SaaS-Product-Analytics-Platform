"""
Global configuration for the AI SaaS Product Analytics Platform.

Every module should import values from this file instead of
hardcoding paths, topic names, or application settings.
"""

from pathlib import Path

# ======================================================
# Project Directories
# ======================================================

PROJECT_ROOT = Path(__file__).resolve().parent.parent

DATA_DIR = PROJECT_ROOT / "data"

RAW_DIR = DATA_DIR / "raw"
BRONZE_DIR = DATA_DIR / "bronze"
SILVER_DIR = DATA_DIR / "silver"
GOLD_DIR = DATA_DIR / "gold"

LOG_DIR = PROJECT_ROOT / "logs"

# ======================================================
# Kafka
# ======================================================

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

KAFKA_TOPIC = "ai-product-events"

# ======================================================
# Spark
# ======================================================

SPARK_APP_NAME = "AI SaaS Product Analytics Platform"

CHECKPOINT_DIR = BRONZE_DIR / "checkpoint"

# ======================================================
# Event Generator
# ======================================================

DEFAULT_RECORDS = 10000

DEFAULT_OUTPUT_FILE = RAW_DIR / "ai_events.csv"