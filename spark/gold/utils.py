"""
AI SaaS Product Analytics Platform
Gold Layer Utility Functions

Shared utility functions used by every Gold layer module.

Responsibilities
----------------
1. Create Spark Session
2. Read Silver Dataset
3. Write Gold Dataset
4. Print formatted logs
5. Common project paths

Compatible with:
- Docker (/app)
- Windows (local execution)
"""

from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import SparkSession


# ==============================================================================
# Project Paths
# ==============================================================================

if Path("/app").exists():
    PROJECT_ROOT = Path("/app")
else:
    PROJECT_ROOT = Path(__file__).resolve().parents[2]

DATA_PATH = PROJECT_ROOT / "data"

SILVER_PATH = DATA_PATH / "silver" / "events"

GOLD_PATH = DATA_PATH / "gold"


# ==============================================================================
# Console Helpers
# ==============================================================================

def print_header(title: str) -> None:
    """
    Print formatted section header.
    """

    print()
    print("=" * 80)
    print(title)
    print("=" * 80)


def print_success(message: str) -> None:
    """
    Print success message.
    """

    print()
    print("✓", message)


# ==============================================================================
# Spark Session
# ==============================================================================

def create_spark_session(app_name: str) -> SparkSession:
    """
    Create Spark Session.

    Parameters
    ----------
    app_name : str

    Returns
    -------
    SparkSession
    """

    spark = (
        SparkSession.builder
        .appName(app_name)
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark


# ==============================================================================
# Read Silver Dataset
# ==============================================================================

def read_silver_data(
    spark: SparkSession,
) -> DataFrame:
    """
    Read Silver Parquet dataset.

    Parameters
    ----------
    spark : SparkSession

    Returns
    -------
    DataFrame
    """

    print_header("Reading Silver Dataset")

    df = spark.read.parquet(str(SILVER_PATH))

    print(f"Silver Path     : {SILVER_PATH}")
    print(f"Records Loaded  : {df.count()}")

    return df


# ==============================================================================
# Write Gold Dataset
# ==============================================================================

def write_gold_table(
    dataframe: DataFrame,
    table_name: str,
) -> None:
    """
    Save Gold dataset.

    Parameters
    ----------
    dataframe : DataFrame

    table_name : str
    """

    output_path = GOLD_PATH / table_name

    (
        dataframe.write
        .mode("overwrite")
        .parquet(str(output_path))
    )

    print_success(f"Gold table saved successfully")

    print(f"Location : {output_path}")


# ==============================================================================
# Dataset Summary
# ==============================================================================

def dataset_summary(
    dataframe: DataFrame,
    dataset_name: str,
) -> None:
    """
    Print dataset summary.
    """

    print()
    print("-" * 80)
    print(f"{dataset_name} Summary")
    print("-" * 80)

    print(f"Rows    : {dataframe.count()}")

    print(f"Columns : {len(dataframe.columns)}")

    dataframe.printSchema()


# ==============================================================================
# Validate Required Columns
# ==============================================================================

def validate_columns(
    dataframe: DataFrame,
    required_columns: list[str],
) -> None:
    """
    Validate DataFrame schema.
    """

    missing = [
        column
        for column in required_columns
        if column not in dataframe.columns
    ]

    if missing:
        raise ValueError(
            f"Missing required columns: {missing}"
        )


# ==============================================================================
# Stop Spark Session
# ==============================================================================

def stop_spark(
    spark: SparkSession,
) -> None:
    """
    Stop Spark Session.
    """

    spark.stop()

    print_success("Spark Session Stopped")