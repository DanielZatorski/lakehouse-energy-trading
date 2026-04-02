import boto3
import os
import json

s3 = boto3.client("s3")
BUCKET_NAME = os.environ["BUCKET_NAME"] #"data-lake-energy-trade" 
BRONZE_PREFIX = "bronze/openweather/current"


def build_s3_key(run_time):
    year = run_time.strftime("%Y")
    month = run_time.strftime("%m")
    day = run_time.strftime("%d")
    hour = run_time.strftime("%H")
    filename = f"current_{run_time.strftime('%Y-%m-%dT%H-%M-%SZ')}.jsonl"

    return (
        f"{BRONZE_PREFIX}/"
        f"year={year}/month={month}/day={day}/hour={hour}/"
        f"{filename}"
    )


def to_jsonl(records, ingested_at):
    lines = []

    for record in records:
        enriched = {
            "ingested_at": ingested_at,
            "source": "open-meteo",
            "dataset": "current",
            **record,
        }
        lines.append(json.dumps(enriched))

    return "\n".join(lines) + "\n"
