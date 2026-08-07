"""
AI SaaS Product Analytics Platform
Gold Layer - User Activity

Generates daily user engagement KPIs.

Output
------
data/gold/user_activity
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
# Build User Activity Metrics
# ==============================================================================

def build_user_activity(df):
    """
    Generate daily user activity metrics.
    """

    user_df = (
        df.withColumn(
            "event_date",
            F.to_date("event_timestamp")
        )
        .groupBy("event_date")
        .agg(
            F.countDistinct("user_id")
            .alias("daily_active_users"),

            F.countDistinct("session_id")
            .alias("total_sessions"),

            F.count("*")
            .alias("total_events"),

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
            "avg_events_per_user",
            F.round(
                F.col("total_events") /
                F.col("daily_active_users"),
                2
            )
        )
        .withColumn(
            "avg_sessions_per_user",
            F.round(
                F.col("total_sessions") /
                F.col("daily_active_users"),
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
        .orderBy("event_date")
    )

    return user_df


# ==============================================================================
# Main
# ==============================================================================

def main():

    print_header(
        "AI SaaS Gold Layer - User Activity"
    )

    spark = create_spark_session(
        "AI-SaaS-User-Activity"
    )

    silver_df = read_silver_data(spark)

    validate_columns(
        silver_df,
        [
            "event_timestamp",
            "user_id",
            "session_id",
            "latency_ms",
            "response_cost",
            "total_tokens",
            "success",
        ]
    )

    user_df = build_user_activity(
        silver_df
    )

    dataset_summary(
        user_df,
        "User Activity"
    )

    user_df.show(
        truncate=False
    )

    write_gold_table(
        user_df,
        "user_activity"
    )

    print_header(
        "USER ACTIVITY CREATED"
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
        print("User Activity Pipeline Failed")
        print("=" * 80)
        print(error)
        raise