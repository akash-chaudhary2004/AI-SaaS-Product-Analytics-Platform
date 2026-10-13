"""Validate row counts for the nine AI SaaS analytics marts in Athena."""

import time

import boto3
from botocore.exceptions import BotoCoreError, ClientError

REGION = "ap-south-1"
DATABASE = "ai_saas_analytics"
RESULTS_LOCATION = "s3://ai-saas-product-analytics-athena-results/"
POLL_INTERVAL_SECONDS = 2
QUERY_TIMEOUT_SECONDS = 120

TABLES = [
    "mart_country_metrics",
    "mart_executive_dashboard",
    "mart_feedback_metrics",
    "mart_model_performance",
    "mart_prompt_metrics",
    "mart_revenue_metrics",
    "mart_session_metrics",
    "mart_subscription_metrics",
    "mart_user_activity",
]


def check_table(client, table):
    """Return the row count for one existing Athena mart."""
    response = client.start_query_execution(
        QueryString=f"SELECT COUNT(*) FROM {DATABASE}.{table}",
        QueryExecutionContext={"Database": DATABASE},
        ResultConfiguration={"OutputLocation": RESULTS_LOCATION},
    )
    query_id = response["QueryExecutionId"]
    deadline = time.monotonic() + QUERY_TIMEOUT_SECONDS

    while time.monotonic() < deadline:
        execution = client.get_query_execution(
            QueryExecutionId=query_id
        )["QueryExecution"]
        status = execution["Status"]["State"]

        if status == "SUCCEEDED":
            result = client.get_query_results(QueryExecutionId=query_id)
            rows = result["ResultSet"]["Rows"]
            return rows[1]["Data"][0].get("VarCharValue", "0")

        if status in {"FAILED", "CANCELLED"}:
            reason = execution["Status"].get(
                "StateChangeReason", "No reason provided"
            )
            raise RuntimeError(f"{status}: {reason}")

        time.sleep(POLL_INTERVAL_SECONDS)

    raise TimeoutError(
        f"Query {query_id} did not finish within "
        f"{QUERY_TIMEOUT_SECONDS} seconds."
    )


def main():
    client = boto3.client("athena", region_name=REGION)
    failures = 0

    for table in TABLES:
        try:
            count = check_table(client, table)
            print(f"{table}: {count} rows")
        except (
            BotoCoreError,
            ClientError,
            RuntimeError,
            TimeoutError,
            KeyError,
            IndexError,
        ) as exc:
            failures += 1
            print(f"{table}: ERROR - {exc}")

    print(f"\nCompleted: {len(TABLES) - failures}/{len(TABLES)} marts checked.")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
