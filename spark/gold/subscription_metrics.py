"""
AI SaaS Product Analytics Platform
Gold Layer - Subscription Metrics

Generates business KPIs grouped by subscription plan.

Output
------
data/gold/subscription_metrics
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
# Build Subscription Metrics
# ==============================================================================

def build_subscription_metrics(df):
    """
    Generate subscription-level business KPIs.
    """

    subscription_df = (
        df.filter(F.col("subscription_plan").isNotNull())
        .groupBy("subscription_plan")
        .agg(
            F.countDistinct("user_id")
            .alias("unique_users"),

            F.countDistinct("session_id")
            .alias("total_sessions"),

            F.count("*")
            .alias("total_events"),

            F.round(
                F.sum("response_cost"),
                4
            ).alias("total_revenue"),

            F.round(
                F.avg("response_cost"),
                4
            ).alias("avg_response_cost"),

            F.round(
                F.avg("latency_ms"),
                2
            ).alias("avg_latency_ms"),

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
            "avg_events_per_user",
            F.round(
                F.col("total_events") /
                F.col("unique_users"),
                2
            )
        )
        .withColumn(
            "avg_sessions_per_user",
            F.round(
                F.col("total_sessions") /
                F.col("unique_users"),
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
            "revenue_per_user",
            F.round(
                F.col("total_revenue") /
                F.col("unique_users"),
                4
            )
        )
        .orderBy(
            F.col("total_revenue").desc()
        )
    )

    return subscription_df


# ==============================================================================
# Main
# ==============================================================================

def main():

    print_header(
        "AI SaaS Gold Layer - Subscription Metrics"
    )

    spark = create_spark_session(
        "AI-SaaS-Subscription-Metrics"
    )

    silver_df = read_silver_data(spark)

    validate_columns(
        silver_df,
        [
            "subscription_plan",
            "user_id",
            "session_id",
            "response_cost",
            "latency_ms",
            "total_tokens",
            "success",
        ]
    )

    subscription_df = build_subscription_metrics(
        silver_df
    )

    dataset_summary(
        subscription_df,
        "Subscription Metrics"
    )

    subscription_df.show(
        truncate=False
    )

    write_gold_table(
        subscription_df,
        "subscription_metrics"
    )

    print_header(
        "SUBSCRIPTION METRICS CREATED"
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
        print("Subscription Metrics Pipeline Failed")
        print("=" * 80)
        print(error)
        raise