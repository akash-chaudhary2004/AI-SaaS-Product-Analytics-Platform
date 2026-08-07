"""
AI SaaS Product Analytics Platform
Gold Layer - Feedback Metrics

Generates user feedback KPIs.

Output
------
data/gold/feedback_metrics
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
# Build Feedback Metrics
# ==============================================================================

def build_feedback_metrics(df):
    """
    Generate user feedback analytics.
    """

    feedback_df = (
        df.filter(F.col("feedback").isNotNull())
        .groupBy("feedback")
        .agg(
            F.count("*")
            .alias("total_feedback"),

            F.countDistinct("user_id")
            .alias("unique_users"),

            F.countDistinct("session_id")
            .alias("total_sessions"),

            F.round(
                F.avg("rating"),
                2
            ).alias("avg_rating"),

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
            "avg_tokens_per_feedback",
            F.round(
                F.col("total_tokens") /
                F.col("total_feedback"),
                2
            )
        )
        .withColumn(
            "avg_revenue_per_feedback",
            F.round(
                F.col("total_revenue") /
                F.col("total_feedback"),
                4
            )
        )
        .orderBy(
            F.col("total_feedback").desc()
        )
    )

    return feedback_df


# ==============================================================================
# Main
# ==============================================================================

def main():

    print_header(
        "AI SaaS Gold Layer - Feedback Metrics"
    )

    spark = create_spark_session(
        "AI-SaaS-Feedback-Metrics"
    )

    silver_df = read_silver_data(spark)

    validate_columns(
        silver_df,
        [
            "feedback",
            "rating",
            "user_id",
            "session_id",
            "latency_ms",
            "response_cost",
            "total_tokens",
            "success",
        ]
    )

    feedback_df = build_feedback_metrics(
        silver_df
    )

    dataset_summary(
        feedback_df,
        "Feedback Metrics"
    )

    feedback_df.show(
        truncate=False
    )

    write_gold_table(
        feedback_df,
        "feedback_metrics"
    )

    print_header(
        "FEEDBACK METRICS CREATED"
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
        print("Feedback Metrics Pipeline Failed")
        print("=" * 80)
        print(error)
        raise