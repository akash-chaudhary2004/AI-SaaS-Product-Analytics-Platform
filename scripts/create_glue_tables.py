import boto3
from botocore.exceptions import ClientError


REGION = "ap-south-1"
DATABASE = "ai_saas_analytics"
BUCKET = "ai-saas-analytics-lakehouse-2026"


TABLES = {
    "country_metrics": [
        ("country", "string"),
        ("unique_users", "bigint"),
        ("paying_users", "bigint"),
        ("total_sessions", "bigint"),
        ("total_events", "bigint"),
        ("total_tokens", "bigint"),
        ("total_ai_cost", "double"),
        ("avg_ai_response_cost", "double"),
        ("avg_latency_ms", "double"),
        ("avg_rating", "double"),
        ("ai_usage_events", "bigint"),
        ("active_ai_users", "bigint"),
        ("active_sessions", "bigint"),
        ("ai_tokens", "bigint"),
        ("total_responses", "bigint"),
        ("success_rate_percent", "double"),
        ("response_avg_latency_ms", "double"),
        ("avg_tokens_per_response", "double"),
        ("response_ai_cost", "double"),
        ("successful_payments", "bigint"),
        ("revenue_generating_users", "bigint"),
        ("payment_revenue", "double"),
        ("revenue_per_user", "double"),
        ("revenue_per_paying_user", "double"),
        ("paid_user_rate_percent", "double"),
        ("avg_events_per_user", "double"),
        ("avg_ai_events_per_user", "double"),
        ("ai_cost_per_user", "double"),
        ("ai_cost_per_response", "double"),
        ("tokens_per_user", "double"),
        ("revenue_minus_ai_cost", "double"),
    ],

    "executive_dashboard": [
        ("total_users", "bigint"),
        ("total_sessions", "bigint"),
        ("total_events", "bigint"),
        ("total_revenue", "double"),
        ("total_tokens", "bigint"),
        ("avg_latency_ms", "double"),
        ("success_rate_percent", "double"),
        ("avg_events_per_user", "double"),
        ("avg_sessions_per_user", "double"),
        ("avg_tokens_per_event", "double"),
        ("avg_revenue_per_event", "double"),
    ],

    "feedback_metrics": [
        ("feedback", "string"),
        ("total_feedback", "bigint"),
        ("unique_users", "bigint"),
        ("total_sessions", "bigint"),
        ("avg_rating", "double"),
        ("avg_latency_ms", "double"),
        ("total_revenue", "double"),
        ("total_tokens", "bigint"),
        ("success_rate_percent", "double"),
        ("avg_tokens_per_feedback", "double"),
        ("avg_revenue_per_feedback", "double"),
    ],

    "model_performance": [
        ("model_name", "string"),
        ("total_events", "bigint"),
        ("unique_users", "bigint"),
        ("total_sessions", "bigint"),
        ("avg_latency_ms", "double"),
        ("min_latency_ms", "int"),
        ("max_latency_ms", "int"),
        ("total_revenue", "double"),
        ("avg_response_cost", "double"),
        ("prompt_tokens", "bigint"),
        ("completion_tokens", "bigint"),
        ("total_tokens", "bigint"),
        ("success_rate_percent", "double"),
        ("avg_tokens_per_request", "double"),
        ("avg_revenue_per_request", "double"),
    ],

    "prompt_metrics": [
        ("prompt_category", "string"),
        ("total_prompts", "bigint"),
        ("unique_users", "bigint"),
        ("total_sessions", "bigint"),
        ("avg_latency_ms", "double"),
        ("min_latency_ms", "int"),
        ("max_latency_ms", "int"),
        ("prompt_tokens", "bigint"),
        ("completion_tokens", "bigint"),
        ("total_tokens", "bigint"),
        ("total_revenue", "double"),
        ("avg_response_cost", "double"),
        ("success_rate_percent", "double"),
        ("avg_tokens_per_prompt", "double"),
        ("avg_revenue_per_prompt", "double"),
    ],

    "revenue_metrics": [
        ("event_date", "date"),
        ("total_revenue", "double"),
        ("avg_revenue_per_response", "double"),
        ("total_tokens", "bigint"),
        ("total_requests", "bigint"),
        ("avg_cost_per_token", "double"),
        ("avg_tokens_per_request", "double"),
    ],

    "session_metrics": [
        ("subscription_plan", "string"),
        ("total_sessions", "bigint"),
        ("unique_users", "bigint"),
        ("total_events", "bigint"),
        ("avg_session_minutes", "double"),
        ("avg_latency_ms", "double"),
        ("total_revenue", "double"),
        ("total_tokens", "bigint"),
        ("success_rate_percent", "double"),
        ("avg_events_per_session", "double"),
        ("avg_tokens_per_session", "double"),
        ("avg_revenue_per_session", "double"),
    ],

    "subscription_metrics": [
        ("subscription_plan", "string"),
        ("total_users", "bigint"),
        ("paying_users", "bigint"),
        ("total_subscriptions", "bigint"),
        ("subscribed_users", "bigint"),
        ("avg_monthly_plan_price", "double"),
        ("calculated_mrr", "double"),
        ("successful_payments", "bigint"),
        ("payment_revenue", "double"),
        ("paying_customers", "bigint"),
        ("total_ai_events", "bigint"),
        ("active_ai_users", "bigint"),
        ("total_sessions", "bigint"),
        ("total_tokens", "bigint"),
        ("avg_latency_ms", "double"),
        ("total_responses", "bigint"),
        ("success_rate_percent", "double"),
        ("avg_ai_response_cost", "double"),
        ("total_ai_response_cost", "double"),
        ("paid_conversion_rate_percent", "double"),
        ("arppu", "double"),
        ("events_per_user", "double"),
        ("avg_tokens_per_response", "double"),
        ("avg_ai_cost_per_response", "double"),
    ],

    "user_activity": [
        ("event_date", "date"),
        ("daily_active_users", "bigint"),
        ("total_sessions", "bigint"),
        ("total_events", "bigint"),
        ("avg_latency_ms", "double"),
        ("total_revenue", "double"),
        ("total_tokens", "bigint"),
        ("success_rate_percent", "double"),
        ("avg_events_per_user", "double"),
        ("avg_sessions_per_user", "double"),
        ("avg_tokens_per_event", "double"),
    ],
}


def table_input(table_name, columns):
    return {
        "Name": table_name,
        "TableType": "EXTERNAL_TABLE",
        "Parameters": {
            "classification": "parquet",
            "EXTERNAL": "TRUE",
            "typeOfData": "file",
        },
        "StorageDescriptor": {
            "Columns": [
                {"Name": name, "Type": dtype}
                for name, dtype in columns
            ],
            "Location": (
                f"s3://{BUCKET}/gold/{table_name}/"
            ),
            "InputFormat": (
                "org.apache.hadoop.hive.ql.io.parquet."
                "MapredParquetInputFormat"
            ),
            "OutputFormat": (
                "org.apache.hadoop.hive.ql.io.parquet."
                "MapredParquetOutputFormat"
            ),
            "SerdeInfo": {
                "SerializationLibrary": (
                    "org.apache.hadoop.hive.ql.io.parquet."
                    "serde.ParquetHiveSerDe"
                ),
                "Parameters": {
                    "serialization.format": "1"
                },
            },
        },
    }


def table_exists(glue, table_name):
    try:
        glue.get_table(
            DatabaseName=DATABASE,
            Name=table_name,
        )
        return True
    except glue.exceptions.EntityNotFoundException:
        return False


def main():
    glue = boto3.client("glue", region_name=REGION)

    print("=" * 80)
    print("GLUE DATA CATALOG TABLE REGISTRATION")
    print("=" * 80)
    print(f"Region:   {REGION}")
    print(f"Database: {DATABASE}")
    print(f"Bucket:   {BUCKET}")
    print(f"Tables:   {len(TABLES)}")
    print("=" * 80)

    for table_name, columns in TABLES.items():
        definition = table_input(table_name, columns)

        try:
            if table_exists(glue, table_name):
                print(f"\nUPDATING: {table_name}")

                glue.update_table(
                    DatabaseName=DATABASE,
                    TableInput=definition,
                )

                print(f"UPDATED:  {table_name}")

            else:
                print(f"\nCREATING: {table_name}")

                glue.create_table(
                    DatabaseName=DATABASE,
                    TableInput=definition,
                )

                print(f"CREATED:  {table_name}")

        except ClientError as exc:
            print(f"\nERROR: {table_name}")
            print(exc)
            raise

    print("\n" + "=" * 80)
    print("GLUE TABLE REGISTRATION COMPLETE")
    print("=" * 80)

    print("\nREGISTERED TABLES:")

    response = glue.get_tables(
        DatabaseName=DATABASE
    )

    for table in sorted(response["TableList"], key=lambda x: x["Name"]):
        print(f" - {table['Name']}")

    print("=" * 80)


if __name__ == "__main__":
    main()