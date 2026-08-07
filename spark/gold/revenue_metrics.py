"""
AI SaaS Product Analytics Platform
Gold Layer - Revenue Metrics

Generates daily revenue KPIs.

Output
------
data/gold/revenue_metrics
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
# Build Revenue Metrics
# ==============================================================================

def build_revenue_metrics(df):
    """
    Generate daily revenue KPIs.
    """

    revenue_df = (
        df.filter(
            F.col("response_cost").isNotNull()
        )
        .withColumn(
            "event_date",
            F.to_date("event_timestamp")
        )
        .groupBy("event_date")
        .agg(
            F.round(
                F.sum("response_cost"),
                4
            ).alias("total_revenue"),

            F.round(
                F.avg("response_cost"),
                4
            ).alias("avg_revenue_per_response"),

            F.sum("total_tokens")
            .alias("total_tokens"),

            F.count("*")
            .alias("total_requests")
        )
        .withColumn(
            "avg_cost_per_token",
            F.round(
                F.col("total_revenue") /
                F.col("total_tokens"),
                8
            )
        )
        .withColumn(
            "avg_tokens_per_request",
            F.round(
                F.col("total_tokens") /
                F.col("total_requests"),
                2
            )
        )
        .orderBy("event_date")
    )

    return revenue_df


# ==============================================================================
# Main
# ==============================================================================

def main():

    print_header(
        "AI SaaS Gold Layer - Revenue Metrics"
    )

    spark = create_spark_session(
        "AI-SaaS-Revenue-Metrics"
    )

    silver_df = read_silver_data(spark)

    validate_columns(
        silver_df,
        [
            "event_timestamp",
            "response_cost",
            "total_tokens",
        ]
    )

    revenue_df = build_revenue_metrics(
        silver_df
    )

    dataset_summary(
        revenue_df,
        "Revenue Metrics"
    )

    revenue_df.show(
        truncate=False
    )

    write_gold_table(
        revenue_df,
        "revenue_metrics"
    )

    print_header(
        "REVENUE METRICS CREATED"
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
        print("Revenue Metrics Pipeline Failed")
        print("=" * 80)
        print(error)
        raise