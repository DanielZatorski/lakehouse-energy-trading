from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator


# Default settings applied to every task in this DAG
default_args = {
    "owner": "daniel",
    "retries": 1,                         # retry once if a task fails
    "retry_delay": timedelta(minutes=5),  # wait 5 minutes before retrying
}


def get_event_date(**context):
    """Returns yesterday's date — the date each Lambda should process."""
    execution_date = context["logical_date"]
    event_date = (execution_date - timedelta(days=1)).strftime("%Y-%m-%d")
    print(f"Processing event_date={event_date}")
    return event_date


with DAG(
    dag_id="entsoe_pipeline",
    description="Bronze → Silver → Gold for ENTSOE energy data",
    default_args=default_args,
    start_date=datetime(2026, 4, 1),   # Airflow won't schedule runs before this date
    schedule="0 2 * * *",              # run daily at 02:00 UTC
    catchup=False,                     # don't backfill missed runs on first start
    tags=["entsoe", "energy"],
) as dag:

    # Task 1 — invoke the bronze Lambda (no date needed, it uses current hour)
    bronze = LambdaInvokeFunctionOperator(
        task_id="bronze_ingestion",
        function_name="data-lake-energy-trade-nordpool",   # your actual Lambda name
        payload="{}",
        aws_conn_id="aws_default",
        region_name="eu-central-1",
    )

    # Task 2 — invoke the silver Lambda for yesterday's date
    silver = LambdaInvokeFunctionOperator(
        task_id="silver_etl",
        function_name="data-lake-energy-trade-silver-entsoe",  # your actual Lambda name
        payload='{"date": "{{ (logical_date - macros.timedelta(days=1)).strftime("%Y-%m-%d") }}"}',
        aws_conn_id="aws_default",
        region_name="eu-central-1",
    )

    # Task 3 — invoke the gold Lambda for yesterday's date
    gold = LambdaInvokeFunctionOperator(
        task_id="gold_etl",
        function_name="data-lake-energy-trade-gold-entsoe",    # your actual Lambda name
        payload='{"date": "{{ (logical_date - macros.timedelta(days=1)).strftime("%Y-%m-%d") }}"}',
        aws_conn_id="aws_default",
        region_name="eu-central-1",
    )

    # silver only starts if bronze succeeded
    # gold only starts if silver succeeded
    bronze >> silver >> gold
