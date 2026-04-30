from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor


default_args = {
    "owner": "daniel",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Date template reused across silver sensors and gold payloads.
# For the scheduled run at 02:00 UTC: bronze fetches the last closed hour of
# the current day, silver writes event_date=today, gold reads event_date=today.
TODAY = '{{ logical_date.strftime("%Y-%m-%d") }}'


with DAG(
    dag_id="entsoe_pipeline",
    description="Bronze → Silver → Gold for ENTSOE and weather data",
    default_args=default_args,
    start_date=datetime(2026, 4, 1),
    schedule="0 2 * * *",       # daily at 02:00 UTC
    catchup=False,
    tags=["entsoe", "weather", "energy"],
) as dag:

    # ── BRONZE ────────────────────────────────────────────────────────────────
    # Bronze Lambdas use the current hour window internally — no date needed

    bronze_entsoe = LambdaInvokeFunctionOperator(
        task_id="bronze_entsoe",
        function_name="data-lake-energy-trade-bronze-nordpool",
        payload="{}",
        aws_conn_id="aws_default",
        region_name="eu-central-1",
    )

    bronze_weather = LambdaInvokeFunctionOperator(
        task_id="bronze_weather",
        function_name="data-lake-energy-trade-bronze-openweather",
        payload="{}",
        aws_conn_id="aws_default",
        region_name="eu-central-1",
    )

    # ── SILVER SENSORS ────────────────────────────────────────────────────────
    # Silver Lambdas are triggered automatically by S3 when bronze writes files.
    # We don't invoke them directly — instead we wait (poke S3 every 60s) until
    # their output Parquet files appear before allowing gold to start.

    wait_silver_entsoe = S3KeySensor(
        task_id="wait_silver_entsoe",
        bucket_name="data-lake-energy-trade",
        bucket_key=f"silver/entsoe/event_date={TODAY}/*",
        wildcard_match=True,
        aws_conn_id="aws_default",
        poke_interval=60,    # check S3 every 60 seconds
        timeout=3600,        # fail the task if silver doesn't appear within 1 hour
    )

    wait_silver_weather = S3KeySensor(
        task_id="wait_silver_weather",
        bucket_name="data-lake-energy-trade",
        bucket_key=f"silver/weather_current/event_date={TODAY}/*",
        wildcard_match=True,
        aws_conn_id="aws_default",
        poke_interval=60,
        timeout=3600,
    )

    # ── GOLD ──────────────────────────────────────────────────────────────────
    # Gold Lambdas take an explicit date so they know which silver partition to read

    gold_entsoe = LambdaInvokeFunctionOperator(
        task_id="gold_entsoe",
        function_name="data-lake-energy-trade-gold-entsoe",
        payload=f'{{"date": "{TODAY}"}}',
        aws_conn_id="aws_default",
        region_name="eu-central-1",
    )

    gold_weather = LambdaInvokeFunctionOperator(
        task_id="gold_weather",
        function_name="data-lake-energy-trade-gold-openweather",
        payload=f'{{"date": "{TODAY}"}}',
        aws_conn_id="aws_default",
        region_name="eu-central-1",
    )

    # ── DEPENDENCY CHAINS ─────────────────────────────────────────────────────
    # Two independent tracks — ENTSOE and weather run fully in parallel

    bronze_entsoe  >> wait_silver_entsoe  >> gold_entsoe
    bronze_weather >> wait_silver_weather >> gold_weather
