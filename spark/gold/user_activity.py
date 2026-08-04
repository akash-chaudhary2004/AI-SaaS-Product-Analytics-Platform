"""
AI SaaS Gold Layer
User Activity Metrics

Reads Silver data and creates
daily user activity metrics.

Output:
data/gold/user_activity
"""

from pyspark.sql import functions as F

from spark.gold.utils import (
    create_spark_session,
    read_silver_data,
    write_gold_table,
    print_header,
)


# ==========================================================
# Main
# ==========================================================

def main():

    print_header("AI SaaS Gold Layer - User Activity Metrics")

    spark = create_spark_session(
        "AI-SaaS-User-Activity"
    )

    df = read_silver_data(spark)

    print(f"Silver records loaded : {df.count()}")

    user_activity = (
        df.groupBy(
            F.to_date("event_timestamp").alias("event_date")
        )
        .agg(
            F.countDistinct("user_id").alias("daily_active_users"),
            F.countDistinct("session_id").alias("total_sessions"),
            F.count("*").alias("total_events"),
            F.round(
                F.count("*") /
                F.countDistinct("user_id"),
                2
            ).alias("avg_events_per_user")
        )
        .orderBy("event_date")
    )

    print(f"Gold rows generated : {user_activity.count()}")

    write_gold_table(
        user_activity,
        "user_activity"
    )

    print_header("USER ACTIVITY GOLD TABLE CREATED")

    user_activity.show(
        truncate=False
    )

    spark.stop()


# ==========================================================
# Entry Point
# ==========================================================

if __name__ == "__main__":
    main()