"""
AI SaaS Product Analytics Platform
Gold Layer - Executive Dashboard

Creates a single-row executive KPI table
for Power BI dashboards.

Output
------
data/gold/executive_dashboard
"""

from pyspark.sql import functions as F

from spark.gold.utils import (
    create_spark_session,
    read_silver_data,
    write_gold_table,
    print_header,
    dataset_summary,
    validate_columns,
    stop_spark,
)


# ==============================================================================
# Build Executive Dashboard
# ==============================================================================

def build_dashboard(df):

    dashboard = (
        df.agg(

            F.countDistinct("user_id")
            .alias("total_users"),

            F.countDistinct("session_id")
            .alias("total_sessions"),

            F.count("*")
            .alias("total_events"),

            F.round(
                F.sum("response_cost"),
                4
            ).alias("total_revenue"),

            F.sum("total_tokens")
            .alias("total_tokens"),

            F.round(
                F.avg("latency_ms"),
                2
            ).alias("avg_latency_ms"),

            F.round(
                F.avg(
                    F.col("success").cast("double")
                ) * 100,
                2
            ).alias("success_rate_percent")

        )
        .withColumn(
            "avg_events_per_user",
            F.round(
                F.col("total_events") /
                F.col("total_users"),
                2
            )
        )
        .withColumn(
            "avg_sessions_per_user",
            F.round(
                F.col("total_sessions") /
                F.col("total_users"),
                2
            )
        )
        .withColumn(
            "avg_tokens_per_event",
            F.round(
                F.col("total_tokens") /
                F.col("total_events"),
                2
            )
        )
        .withColumn(
            "avg_revenue_per_event",
            F.round(
                F.col("total_revenue") /
                F.col("total_events"),
                4
            )
        )
    )

    return dashboard


# ==============================================================================
# Main
# ==============================================================================

def main():

    print_header(
        "AI SaaS Gold Layer - Executive Dashboard"
    )

    spark = create_spark_session(
        "AI-SaaS-Executive-Dashboard"
    )

    silver_df = read_silver_data(spark)

    validate_columns(
        silver_df,
        [
            "user_id",
            "session_id",
            "response_cost",
            "total_tokens",
            "latency_ms",
            "success",
        ]
    )

    dashboard_df = build_dashboard(
        silver_df
    )

    dataset_summary(
        dashboard_df,
        "Executive Dashboard"
    )

    dashboard_df.show(
        truncate=False
    )

    write_gold_table(
        dashboard_df,
        "executive_dashboard"
    )

    print_header(
        "EXECUTIVE DASHBOARD CREATED"
    )

    stop_spark(spark)


# ==============================================================================
# Entry Point
# ==============================================================================

if __name__ == "__main__":

    try:
        main()

    except Exception as error:

        print()
        print("=" * 80)
        print("Executive Dashboard Pipeline Failed")
        print("=" * 80)
        print(error)
        raise