"""
AI SaaS Product Analytics Platform
Gold Layer - Subscription Metrics

Purpose
-------
Create subscription-level business KPIs from the Silver dataset.

This module focuses on SaaS subscription and monetization analytics.

Output
------
data/gold/subscription_metrics

Key KPIs
--------
- Total users
- Paying users
- Total subscriptions
- Active subscriptions
- Paid conversion rate
- Monthly recurring revenue (MRR)
- Payment revenue
- Average revenue per paying user (ARPPU)
- Average plan price
- Total AI events
- Total AI tokens
- Average latency
- Success rate
"""

from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from spark.gold.utils import (
    create_spark_session,
    read_silver_data,
    write_gold_table,
    print_header,
)


# ==============================================================================
# Configuration
# ==============================================================================

APP_NAME = "AI-SaaS-Gold-Subscription-Metrics"

OUTPUT_TABLE = "subscription_metrics"

PAID_PLANS = [
    "Plus",
    "Pro",
    "Enterprise",
]

AI_USAGE_EVENTS = [
    "prompt_submitted",
    "response_generated",
    "chat_started",
]


# ==============================================================================
# Build Subscription Metrics
# ==============================================================================

def build_subscription_metrics(
    df: DataFrame,
) -> DataFrame:
    """
    Build subscription-level SaaS KPIs.

    One row is produced for each subscription plan.
    """

    print_header(
        "BUILDING SUBSCRIPTION METRICS"
    )

    # --------------------------------------------------------------------------
    # User-level metrics
    # --------------------------------------------------------------------------

    user_metrics = (
        df
        .filter(
            F.col("user_id").isNotNull()
        )
        .groupBy(
            "subscription_plan"
        )
        .agg(
            F.countDistinct(
                "user_id"
            ).alias(
                "total_users"
            ),

            F.countDistinct(
                F.when(
                    F.col("subscription_plan").isin(
                        PAID_PLANS
                    ),
                    F.col("user_id")
                )
            ).alias(
                "paying_users"
            ),
        )
    )

    # --------------------------------------------------------------------------
    # Subscription metrics
    #
    # subscription_created is the business event representing a new
    # subscription. We don't count every Silver row as a subscription.
    # --------------------------------------------------------------------------

    subscription_metrics = (
        df
        .filter(
            F.col("event_type") == "subscription_created"
        )
        .groupBy(
            "subscription_plan"
        )
        .agg(
            F.countDistinct(
                "subscription_id"
            ).alias(
                "total_subscriptions"
            ),

            F.countDistinct(
                "user_id"
            ).alias(
                "subscribed_users"
            ),

            F.round(
                F.avg(
                    F.coalesce(
                        F.col("monthly_price"),
                        F.col("plan_price")
                    )
                ),
                2
            ).alias(
                "avg_monthly_plan_price"
            ),

            F.round(
                F.sum(
                    F.coalesce(
                        F.col("monthly_price"),
                        F.col("plan_price")
                    )
                ),
                2
            ).alias(
                "calculated_mrr"
            ),
        )
    )

    # --------------------------------------------------------------------------
    # Payment metrics
    #
    # Actual payment revenue comes from payment_success events.
    # response_cost is AI infrastructure cost, NOT customer revenue.
    # --------------------------------------------------------------------------

    payment_metrics = (
        df
        .filter(
            F.col("event_type") == "payment_success"
        )
        .groupBy(
            "subscription_plan"
        )
        .agg(
            F.countDistinct(
                "payment_id"
            ).alias(
                "successful_payments"
            ),

            F.round(
                F.sum(
                    F.coalesce(
                        F.col("amount"),
                        F.lit(0.0)
                    )
                ),
                2
            ).alias(
                "payment_revenue"
            ),

            F.countDistinct(
                "user_id"
            ).alias(
                "paying_customers"
            ),
        )
    )

    # --------------------------------------------------------------------------
    # AI usage metrics
    # --------------------------------------------------------------------------

    usage_metrics = (
        df
        .filter(
            F.col("event_type").isin(
                AI_USAGE_EVENTS
            )
        )
        .groupBy(
            "subscription_plan"
        )
        .agg(
            F.count("*").alias(
                "total_ai_events"
            ),

            F.countDistinct(
                "user_id"
            ).alias(
                "active_ai_users"
            ),

            F.countDistinct(
                "session_id"
            ).alias(
                "total_sessions"
            ),

            F.sum(
                F.coalesce(
                    F.col("total_tokens"),
                    F.lit(0)
                )
            ).alias(
                "total_tokens"
            ),

            F.round(
                F.avg(
                    "latency_ms"
                ),
                2
            ).alias(
                "avg_latency_ms"
            ),
        )
    )

    # --------------------------------------------------------------------------
    # Response quality metrics
    #
    # Only response_generated events have meaningful response success
    # and latency values.
    # --------------------------------------------------------------------------

    quality_metrics = (
        df
        .filter(
            F.col("event_type") == "response_generated"
        )
        .groupBy(
            "subscription_plan"
        )
        .agg(
            F.count("*").alias(
                "total_responses"
            ),

            F.round(
                F.avg(
                    F.col("success").cast("double")
                ) * 100,
                2
            ).alias(
                "success_rate_percent"
            ),

            F.round(
                F.avg(
                    "response_cost"
                ),
                6
            ).alias(
                "avg_ai_response_cost"
            ),

            F.round(
                F.sum(
                    F.coalesce(
                        F.col("response_cost"),
                        F.lit(0.0)
                    )
                ),
                4
            ).alias(
                "total_ai_response_cost"
            ),
        )
    )

    # --------------------------------------------------------------------------
    # Combine metrics
    # --------------------------------------------------------------------------

    result = (
        user_metrics
        .join(
            subscription_metrics,
            on="subscription_plan",
            how="left",
        )
        .join(
            payment_metrics,
            on="subscription_plan",
            how="left",
        )
        .join(
            usage_metrics,
            on="subscription_plan",
            how="left",
        )
        .join(
            quality_metrics,
            on="subscription_plan",
            how="left",
        )
    )

    # --------------------------------------------------------------------------
    # Derived business KPIs
    # --------------------------------------------------------------------------

    result = (
        result

        # --------------------------------------------------------------
        # Paid conversion
        # --------------------------------------------------------------

        .withColumn(
            "paid_conversion_rate_percent",
            F.when(
                F.col("total_users") > 0,
                F.round(
                    (
                        F.col("paying_users")
                        /
                        F.col("total_users")
                    ) * 100,
                    2,
                ),
            ).otherwise(
                F.lit(0.0)
            ),
        )

        # --------------------------------------------------------------
        # ARPPU
        # --------------------------------------------------------------

        .withColumn(
            "arppu",
            F.when(
                F.col("paying_customers") > 0,
                F.round(
                    F.col("payment_revenue")
                    /
                    F.col("paying_customers"),
                    2,
                ),
            ).otherwise(
                F.lit(0.0)
            ),
        )

        # --------------------------------------------------------------
        # Events per user
        # --------------------------------------------------------------

        .withColumn(
            "events_per_user",
            F.when(
                F.col("active_ai_users") > 0,
                F.round(
                    F.col("total_ai_events")
                    /
                    F.col("active_ai_users"),
                    2,
                ),
            ).otherwise(
                F.lit(0.0)
            ),
        )

        # --------------------------------------------------------------
        # Tokens per response
        # --------------------------------------------------------------

        .withColumn(
            "avg_tokens_per_response",
            F.when(
                F.col("total_responses") > 0,
                F.round(
                    F.col("total_tokens")
                    /
                    F.col("total_responses"),
                    2,
                ),
            ).otherwise(
                F.lit(0.0)
            ),
        )

        # --------------------------------------------------------------
        # AI cost per response
        # --------------------------------------------------------------

        .withColumn(
            "avg_ai_cost_per_response",
            F.when(
                F.col("total_responses") > 0,
                F.round(
                    F.col("total_ai_response_cost")
                    /
                    F.col("total_responses"),
                    6,
                ),
            ).otherwise(
                F.lit(0.0)
            ),
        )

        # --------------------------------------------------------------
        # Fill metric NULLs
        # --------------------------------------------------------------

        .fillna(
            0,
            subset=[
                "total_subscriptions",
                "subscribed_users",
                "successful_payments",
                "payment_revenue",
                "paying_customers",
                "total_ai_events",
                "active_ai_users",
                "total_sessions",
                "total_tokens",
                "total_responses",
                "total_ai_response_cost",
            ],
        )

        # --------------------------------------------------------------
        # Business-friendly ordering
        # --------------------------------------------------------------

        .orderBy(
            F.when(
                F.col("subscription_plan") == "Enterprise",
                1,
            )
            .when(
                F.col("subscription_plan") == "Pro",
                2,
            )
            .when(
                F.col("subscription_plan") == "Plus",
                3,
            )
            .otherwise(4)
        )
    )

    return result


# ==============================================================================
# Main
# ==============================================================================

def main():

    print_header(
        "AI SaaS GOLD - SUBSCRIPTION METRICS"
    )

    spark = None

    try:

        # ----------------------------------------------------------------------
        # Spark
        # ----------------------------------------------------------------------

        spark = create_spark_session(
            APP_NAME
        )

        # ----------------------------------------------------------------------
        # Silver
        # ----------------------------------------------------------------------

        silver_df = read_silver_data(
            spark
        )

        print(
            f"Silver records loaded : "
            f"{silver_df.count()}"
        )

        # ----------------------------------------------------------------------
        # Metrics
        # ----------------------------------------------------------------------

        subscription_df = build_subscription_metrics(
            silver_df
        )

        # ----------------------------------------------------------------------
        # Output
        # ----------------------------------------------------------------------

        print_header(
            "SUBSCRIPTION METRICS RESULT"
        )

        print(
            f"Gold rows generated : "
            f"{subscription_df.count()}"
        )

        subscription_df.show(
            truncate=False
        )

        # ----------------------------------------------------------------------
        # Write Gold
        # ----------------------------------------------------------------------

        write_gold_table(
            subscription_df,
            OUTPUT_TABLE,
        )

        print_header(
            "SUBSCRIPTION METRICS COMPLETED"
        )

    except Exception as error:

        print()
        print("=" * 80)
        print(
            "ERROR: SUBSCRIPTION METRICS FAILED"
        )
        print("=" * 80)

        print(
            f"{type(error).__name__}: {error}"
        )

        raise

    finally:

        if spark is not None:
            spark.stop()

            print(
                "Spark session stopped."
            )


# ==============================================================================
# Entry Point
# ==============================================================================

if __name__ == "__main__":
    main()