"""
AI SaaS Product Analytics Platform
Gold Layer - Prompt Metrics

Generates prompt-level analytics by prompt category.

Output
------
data/gold/prompt_metrics
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
# Build Prompt Metrics
# ==============================================================================

def build_prompt_metrics(df):
    """
    Generate KPIs for each prompt category.
    """

    prompt_df = (
        df.filter(F.col("prompt_category").isNotNull())
        .groupBy("prompt_category")
        .agg(
            F.count("*")
            .alias("total_prompts"),

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

            F.sum("prompt_tokens")
            .alias("prompt_tokens"),

            F.sum("completion_tokens")
            .alias("completion_tokens"),

            F.sum("total_tokens")
            .alias("total_tokens"),

            F.round(
                F.sum("response_cost"),
                4
            ).alias("total_revenue"),

            F.round(
                F.avg("response_cost"),
                4
            ).alias("avg_response_cost"),

            F.round(
                F.avg(
                    F.col("success").cast("double")
                ) * 100,
                2
            ).alias("success_rate_percent")
        )
        .withColumn(
            "avg_tokens_per_prompt",
            F.round(
                F.col("total_tokens") /
                F.col("total_prompts"),
                2
            )
        )
        .withColumn(
            "avg_revenue_per_prompt",
            F.round(
                F.col("total_revenue") /
                F.col("total_prompts"),
                4
            )
        )
        .orderBy(
            F.col("total_prompts").desc()
        )
    )

    return prompt_df


# ==============================================================================
# Main
# ==============================================================================

def main():

    print_header(
        "AI SaaS Gold Layer - Prompt Metrics"
    )

    spark = create_spark_session(
        "AI-SaaS-Prompt-Metrics"
    )

    silver_df = read_silver_data(spark)

    validate_columns(
        silver_df,
        [
            "prompt_category",
            "user_id",
            "session_id",
            "latency_ms",
            "prompt_tokens",
            "completion_tokens",
            "total_tokens",
            "response_cost",
            "success",
        ]
    )

    prompt_df = build_prompt_metrics(
        silver_df
    )

    dataset_summary(
        prompt_df,
        "Prompt Metrics"
    )

    prompt_df.show(
        truncate=False
    )

    write_gold_table(
        prompt_df,
        "prompt_metrics"
    )

    print_header(
        "PROMPT METRICS CREATED"
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
        print("Prompt Metrics Pipeline Failed")
        print("=" * 80)
        print(error)
        raise