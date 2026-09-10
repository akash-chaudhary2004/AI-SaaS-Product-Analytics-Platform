import json
import subprocess
import time


DATABASE = "ai_saas_analytics"
WORKGROUP = "primary"
OUTPUT_LOCATION = (
    "s3://ai-saas-analytics-lakehouse-2026/athena-results/"
)


DATASETS = {
    "country_metrics": [
        "COUNT(*)",
        "SUM(unique_users)",
        "SUM(paying_users)",
        "SUM(total_sessions)",
        "SUM(total_events)",
        "SUM(total_tokens)",
        "SUM(total_ai_cost)",
        "SUM(payment_revenue)",
        "SUM(revenue_minus_ai_cost)",
        "AVG(avg_latency_ms)",
        "AVG(success_rate_percent)",
    ],
    "executive_dashboard": [
        "COUNT(*)",
        "SUM(total_users)",
        "SUM(total_sessions)",
        "SUM(total_events)",
        "SUM(total_revenue)",
        "SUM(total_tokens)",
        "AVG(avg_latency_ms)",
        "AVG(success_rate_percent)",
    ],
    "feedback_metrics": [
        "COUNT(*)",
        "SUM(total_feedback)",
        "SUM(unique_users)",
        "SUM(total_sessions)",
        "AVG(avg_rating)",
    ],
    "model_performance": [
        "COUNT(*)",
        "SUM(total_events)",
        "SUM(unique_users)",
        "SUM(total_sessions)",
        "SUM(total_revenue)",
        "SUM(prompt_tokens)",
        "SUM(completion_tokens)",
        "SUM(total_tokens)",
        "AVG(avg_latency_ms)",
        "AVG(success_rate_percent)",
    ],
    "prompt_metrics": [
        "COUNT(*)",
        "SUM(total_prompts)",
        "SUM(unique_users)",
        "SUM(total_sessions)",
        "SUM(prompt_tokens)",
        "SUM(completion_tokens)",
        "SUM(total_tokens)",
        "SUM(total_revenue)",
        "AVG(avg_latency_ms)",
        "AVG(success_rate_percent)",
    ],
    "revenue_metrics": [
        "COUNT(*)",
        "SUM(total_revenue)",
        "SUM(total_tokens)",
        "SUM(total_requests)",
        "AVG(avg_cost_per_token)",
        "AVG(avg_tokens_per_request)",
    ],
    "session_metrics": [
        "COUNT(*)",
        "SUM(total_sessions)",
        "SUM(unique_users)",
        "SUM(total_events)",
        "SUM(total_revenue)",
        "SUM(total_tokens)",
        "AVG(avg_session_minutes)",
        "AVG(avg_latency_ms)",
        "AVG(success_rate_percent)",
    ],
    "subscription_metrics": [
        "COUNT(*)",
        "SUM(total_users)",
        "SUM(paying_users)",
        "SUM(total_subscriptions)",
        "SUM(successful_payments)",
        "SUM(payment_revenue)",
        "SUM(total_ai_events)",
        "SUM(total_sessions)",
        "SUM(total_tokens)",
        "SUM(total_ai_response_cost)",
    ],
    "user_activity": [
        "COUNT(*)",
        "MIN(event_date)",
        "MAX(event_date)",
        "SUM(daily_active_users)",
        "SUM(total_sessions)",
        "SUM(total_events)",
        "SUM(total_revenue)",
        "SUM(total_tokens)",
        "AVG(avg_latency_ms)",
        "AVG(success_rate_percent)",
    ],
}


def run_aws_cli(args):
    result = subprocess.run(
        ["aws"] + args,
        capture_output=True,
        text=True,
        check=True,
    )

    return json.loads(result.stdout)


def start_query(query):
    response = run_aws_cli(
        [
            "athena",
            "start-query-execution",
            "--query-string",
            query,
            "--work-group",
            WORKGROUP,
            "--query-execution-context",
            f"Database={DATABASE}",
            "--result-configuration",
            f"OutputLocation={OUTPUT_LOCATION}",
        ]
    )

    return response["QueryExecutionId"]


def wait_for_query(query_id):
    while True:
        response = run_aws_cli(
            [
                "athena",
                "get-query-execution",
                "--query-execution-id",
                query_id,
            ]
        )

        state = response["QueryExecution"]["Status"]["State"]

        if state == "SUCCEEDED":
            return

        if state in {"FAILED", "CANCELLED"}:
            reason = response["QueryExecution"]["Status"].get(
                "StateChangeReason",
                "Unknown Athena error",
            )
            raise RuntimeError(
                f"Athena query {state}: {reason}"
            )

        time.sleep(2)


def get_results(query_id):
    response = run_aws_cli(
        [
            "athena",
            "get-query-results",
            "--query-execution-id",
            query_id,
        ]
    )

    return response["ResultSet"]["Rows"]


def normalize(value):
    if value is None or value == "":
        return None

    try:
        return float(value)
    except ValueError:
        return value


def values_match(gold, dbt):
    gold = normalize(gold)
    dbt = normalize(dbt)

    if gold is None or dbt is None:
        return gold == dbt

    if isinstance(gold, float) and isinstance(dbt, float):
        return abs(gold - dbt) <= 1e-9

    return gold == dbt


def validate_dataset(name, expressions):
    print("\n" + "-" * 80)
    print(f"DATASET: {name}")
    print("-" * 80)

    gold_select = ", ".join(expressions)

    dbt_select = ", ".join(expressions)

    query = f"""
    SELECT
        'gold' AS layer,
        {gold_select}
    FROM {name}

    UNION ALL

    SELECT
        'dbt' AS layer,
        {dbt_select}
    FROM mart_{name}
    """

    query_id = start_query(query)
    wait_for_query(query_id)

    rows = get_results(query_id)

    if len(rows) < 3:
        raise RuntimeError(
            f"Unexpected Athena result for {name}"
        )

    gold_values = [
        item.get("VarCharValue")
        for item in rows[1]["Data"]
    ]

    dbt_values = [
        item.get("VarCharValue")
        for item in rows[2]["Data"]
    ]

    # First value is the layer name.
    gold_values = gold_values[1:]
    dbt_values = dbt_values[1:]

    dataset_passed = True

    for expression, gold, dbt in zip(
        expressions,
        gold_values,
        dbt_values,
    ):
        if values_match(gold, dbt):
            print(
                f"PASS  {expression}: {gold}"
            )
        else:
            print(
                f"FAIL  {expression}: "
                f"Gold={gold}, dbt={dbt}"
            )
            dataset_passed = False

    if dataset_passed:
        print(f"\nRESULT: {name} → PASS")
    else:
        print(f"\nRESULT: {name} → FAIL")

    return dataset_passed


def main():
    print("=" * 80)
    print("AUTOMATED GOLD vs DBT VALIDATION")
    print("=" * 80)
    print(f"Database : {DATABASE}")
    print(f"Datasets : {len(DATASETS)}")

    all_passed = True

    for name, expressions in DATASETS.items():
        try:
            passed = validate_dataset(
                name,
                expressions,
            )

            if not passed:
                all_passed = False

        except Exception as error:
            print(f"\nRESULT: {name} → ERROR")
            print(f"ERROR: {error}")
            all_passed = False

    print("\n" + "=" * 80)

    if all_passed:
        print("FINAL RESULT: GOLD vs DBT VALIDATION PASSED")
        print("All 9 datasets match.")
    else:
        print("FINAL RESULT: GOLD vs DBT VALIDATION FAILED")
        print("Review the dataset-level failures above.")

    print("=" * 80)

    return 0 if all_passed else 1


if __name__ == "__main__":
    raise SystemExit(main())