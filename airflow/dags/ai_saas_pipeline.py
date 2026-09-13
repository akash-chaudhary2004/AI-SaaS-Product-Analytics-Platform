from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="ai_saas_analytics_pipeline",
    description="Orchestrates Silver, Gold, dbt, and validation pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["ai-saas", "spark", "dbt", "analytics"],
) as dag:

    silver = BashOperator(
        task_id="silver_processing",
        bash_command=(
            "docker exec spark bash -lc "
            "'cd /app && PYTHONPATH=/app "
            "/opt/spark/bin/spark-submit "
            "spark/silver/silver_stream.py'"
        ),
    )

    gold = BashOperator(
        task_id="gold_generation",
        bash_command=(
            "docker exec spark bash -lc "
            "'cd /app && PYTHONPATH=/app "
            "/opt/spark/bin/spark-submit "
            "spark/gold/gold_runner.py'"
        ),
    )

    dbt = BashOperator(
        task_id="dbt_build",
        bash_command=(
            "docker exec airflow-dbt "
            "dbt build --project-dir /dbt/project --no-partial-parse"
        ),
    )

    silver >> gold >> dbt
