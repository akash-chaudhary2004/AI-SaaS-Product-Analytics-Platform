"""
AI SaaS Product Analytics Platform
Gold Layer - Session Metrics

Generates subscription-level session KPIs.

Output
------
data/gold/session_metrics
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
# Build Session Metrics
# ==============================================================================

def build_session_metrics(df):
    """
    Generate session KPIs grouped by subscription plan.
    """

    session_df = (
        df.filter(
            F.col("subscription_plan").isNotNull()
        )
        .groupBy("subscription_plan")
        .agg(
            F.countDistinct("session_id")
            .alias("total_sessions"),

            F.countDistinct("user_id")
            .alias("unique_users"),

            F.count("*")
            .alias("total_events"),

            F.round(
                F.avg("session_duration_minutes"),
                2
            ).alias("avg_session_minutes"),

            F.round(
                F.avg("latency_ms"),
                2
            ).alias("avg_latency_ms"),

            F.round(
                F.sum("response_cost"),
                4
            ).alias("total_revenue"),

            F.sum("total_tokens")
            .alias("total_tokens"),

            F.round(
                F.avg(
                    F.col("success").cast("double")
                ) * 100,
                2
            ).alias("success_rate_percent")
        )
        .withColumn(
            "avg_events_per_session",
            F.round(
                F.col("total_events") /
                F.col("total_sessions"),
                2
            )
        )
        .withColumn(
            "avg_tokens_per_session",
            F.round(
                F.col("total_tokens") /
                F.col("total_sessions"),
                2
            )
        )
        .withColumn(
            "avg_revenue_per_session",
            F.round(
                F.col("total_revenue") /
                F.col("total_sessions"),
                4
            )
        )
        .orderBy(
            F.col("total_sessions").desc()
        )
    )

    return session_df


# ==============================================================================
# Main
# ==============================================================================

def main():

    print_header(
        "AI SaaS Gold Layer - Session Metrics"
    )

    spark = create_spark_session(
        "AI-SaaS-Session-Metrics"
    )

    silver_df = read_silver_data(spark)

    validate_columns(
        silver_df,
        [
            "subscription_plan",
            "session_id",
            "user_id",
            "session_duration_minutes",
            "latency_ms",
            "response_cost",
            "total_tokens",
            "success",
        ]
    )

    session_df = build_session_metrics(
        silver_df
    )

    dataset_summary(
        session_df,
        "Session Metrics"
    )

    session_df.show(
        truncate=False
    )

    write_gold_table(
        session_df,
        "session_metrics"
    )

    print_header(
        "SESSION METRICS CREATED"
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
        print("Session Metrics Pipeline Failed")
        print("=" * 80)
        print(error)
        raise