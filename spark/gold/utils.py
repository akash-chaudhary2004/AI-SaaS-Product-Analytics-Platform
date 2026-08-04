"""
Gold Layer Utility Functions

Shared helper functions used by all Gold modules.

Responsibilities
----------------
1. Create Spark Session
2. Read Silver Dataset
3. Write Gold Dataset
4. Logging
"""

from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import DataFrame


# ==========================================================
# Project Paths
# ==========================================================

from pathlib import Path

# Works in both Docker and Windows
if Path("/app").exists():
    PROJECT_ROOT = Path("/app")
else:
    PROJECT_ROOT = Path(__file__).resolve().parents[2]

SILVER_PATH = PROJECT_ROOT / "data" / "silver" / "events"

GOLD_PATH = PROJECT_ROOT / "data" / "gold"

# ==========================================================
# Spark Session
# ==========================================================

def create_spark_session(app_name: str) -> SparkSession:
    """
    Create Spark session for Gold layer.
    """

    spark = (
        SparkSession.builder
        .appName(app_name)
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark


# ==========================================================
# Read Silver Dataset
# ==========================================================

def read_silver_data(spark: SparkSession) -> DataFrame:
    """
    Read cleaned Silver dataset.
    """

    print("=" * 80)
    print("Reading Silver Dataset")
    print("=" * 80)

    df = spark.read.parquet(str(SILVER_PATH))

    print(f"Records Loaded : {df.count()}")

    return df


# ==========================================================
# Write Gold Dataset
# ==========================================================

def write_gold_table(
    dataframe: DataFrame,
    table_name: str,
):
    """
    Save one Gold dataset.

    Example
    -------
    data/gold/user_activity/
    """

    output_path = GOLD_PATH / table_name

    (
        dataframe.write
        .mode("overwrite")
        .parquet(str(output_path))
    )

    print(f"Saved : {output_path}")


# ==========================================================
# Section Header
# ==========================================================

def print_header(title: str):
    """
    Print formatted section header.
    """

    print()
    print("=" * 80)
    print(title)
    print("=" * 80)