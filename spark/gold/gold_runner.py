"""
AI SaaS Product Analytics Platform
Gold Layer Runner

Reads the Silver dataset once,
caches it in memory,
generates every Gold table,
then stops Spark.

Author: Akash Kumar
"""

from spark.gold.utils import (
    create_spark_session,
    read_silver_data,
    write_gold_table,
    print_header,
    stop_spark,
)

from spark.gold.revenue_metrics import build_revenue_metrics
from spark.gold.session_metrics import build_session_metrics
from spark.gold.subscription_metrics import build_subscription_metrics
from spark.gold.country_metrics import build_country_metrics
from spark.gold.model_performance import build_model_performance
from spark.gold.prompt_metrics import build_prompt_metrics
from spark.gold.feedback_metrics import build_feedback_metrics
from spark.gold.user_activity import build_user_activity
from spark.gold.executive_dashboard import build_dashboard


# ==============================================================================
# Helper
# ==============================================================================

def generate_table(name, builder, silver_df):
    """
    Generate one Gold table.
    """

    print()
    print("=" * 80)
    print(f"Generating {name}")
    print("=" * 80)

    gold_df = builder(silver_df)

    print(f"Rows Generated : {gold_df.count()}")

    gold_df.show(10, truncate=False)

    write_gold_table(
        gold_df,
        name
    )

    print(f"{name} completed successfully.")


# ==============================================================================
# Main
# ==============================================================================

def main():

    print_header(
        "AI SaaS Product Analytics Platform\nGold Layer Pipeline"
    )

    spark = create_spark_session(
        "AI-SaaS-Gold-Runner"
    )

    print("Reading Silver dataset...")

    silver_df = read_silver_data(
        spark
    )

    print()
    print("Caching Silver dataset...")

    silver_df.cache()

    print(
        f"Cached Records : {silver_df.count()}"
    )

    # --------------------------------------------------------------------------
    # Gold Tables
    # --------------------------------------------------------------------------

    generate_table(
        "revenue_metrics",
        build_revenue_metrics,
        silver_df
    )

    generate_table(
        "session_metrics",
        build_session_metrics,
        silver_df
    )

    generate_table(
        "subscription_metrics",
        build_subscription_metrics,
        silver_df
    )

    generate_table(
        "country_metrics",
        build_country_metrics,
        silver_df
    )

    generate_table(
        "model_performance",
        build_model_performance,
        silver_df
    )

    generate_table(
        "prompt_metrics",
        build_prompt_metrics,
        silver_df
    )

    generate_table(
        "feedback_metrics",
        build_feedback_metrics,
        silver_df
    )

    generate_table(
        "user_activity",
        build_user_activity,
        silver_df
    )

    generate_table(
        "executive_dashboard",
        build_dashboard,
        silver_df
    )

    # --------------------------------------------------------------------------
    # Cleanup
    # --------------------------------------------------------------------------

    print()
    print("Removing cached Silver dataset...")

    silver_df.unpersist()

    print_header(
        "ALL GOLD TABLES GENERATED SUCCESSFULLY"
    )

    stop_spark(
        spark
    )


# ==============================================================================
# Entry Point
# ==============================================================================

if __name__ == "__main__":

    try:
        main()

    except Exception as error:

        print()
        print("=" * 80)
        print("GOLD PIPELINE FAILED")
        print("=" * 80)
        print(error)
        raise