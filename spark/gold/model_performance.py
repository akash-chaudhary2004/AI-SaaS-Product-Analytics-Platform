"""
AI SaaS Product Analytics Platform
Gold Layer - Model Performance

Generates AI model performance KPIs.

Output
------
data/gold/model_performance
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
# Build Model Performance Metrics
# ==============================================================================

def build_model_performance(df):
    """
    Generate performance metrics for each AI model.
    """

    model_df = (
        df.filter(F.col("model_name").isNotNull())
        .groupBy("model_name")
        .agg(
            F.count("*")
            .alias("total_events"),

            F.countDistinct("user_id")
            .alias("unique_users"),

            F.countDistinct("session_id")
            .alias("total_sessions"),

            F.round(
                F.avg("latency_ms"),
                2
            ).alias("avg_latency_ms"),

            F.round(
                F.min("latency_ms"),
                2
            ).alias("min_latency_ms"),

            F.round(
                F.max("latency_ms"),
                2
            ).alias("max_latency_ms"),

            F.round(
                F.sum("response_cost"),
                4
            ).alias("total_revenue"),

            F.round(
                F.avg("response_cost"),
                4
            ).alias("avg_response_cost"),

            F.sum("prompt_tokens")
            .alias("prompt_tokens"),

            F.sum("completion_tokens")
            .alias("completion_tokens"),

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
            "avg_tokens_per_request",
            F.round(
                F.col("total_tokens") /
                F.col("total_events"),
                2
            )
        )
        .withColumn(
            "avg_revenue_per_request",
            F.round(
                F.col("total_revenue") /
                F.col("total_events"),
                4
            )
        )
        .orderBy(
            F.col("total_revenue").desc()
        )
    )

    return model_df


# ==============================================================================
# Main
# ==============================================================================

def main():

    print_header(
        "AI SaaS Gold Layer - Model Performance"
    )

    spark = create_spark_session(
        "AI-SaaS-Model-Performance"
    )

    silver_df = read_silver_data(spark)

    validate_columns(
        silver_df,
        [
            "model_name",
            "user_id",
            "session_id",
            "latency_ms",
            "response_cost",
            "prompt_tokens",
            "completion_tokens",
            "total_tokens",
            "success",
        ]
    )

    model_df = build_model_performance(
        silver_df
    )

    dataset_summary(
        model_df,
        "Model Performance"
    )

    model_df.show(
        truncate=False
    )

    write_gold_table(
        model_df,
        "model_performance"
    )

    print_header(
        "MODEL PERFORMANCE CREATED"
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
        print("Model Performance Pipeline Failed")
        print("=" * 80)
        print(error)
        raise