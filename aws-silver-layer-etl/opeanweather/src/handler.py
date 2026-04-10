import json
import io
import os
from datetime import datetime, timezone
from urllib.parse import unquote_plus
from botocore.exceptions import ClientError
import boto3
import pandas as pd

s3 = boto3.client("s3")

SILVER_BUCKET = os.environ["SILVER_BUCKET"]
SILVER_PREFIX = os.environ["SILVER_PREFIX"]


def read_jsonl_from_s3(bucket: str, key: str):
    response = s3.get_object(Bucket=bucket, Key=key)
    content = response["Body"].read().decode("utf-8")

    rows = []
    for line in content.splitlines():
        line = line.strip()
        if not line:
            continue
        rows.append(json.loads(line))
    return rows


def normalize_timestamp(ts: str):
    if not ts:
        return None
    return pd.to_datetime(ts, utc=True)


ROUND_FIELDS = {
    "response_lat",
    "response_lon",
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation",
    "surface_pressure",
    "cloud_cover",
    "shortwave_radiation",
    "direct_radiation",
    "diffuse_radiation",
    "global_tilted_irradiance",
    "wind_speed_100m",
    "wind_direction_100m",
    "wind_gusts_10m",
}

def round_if_number(value, ndigits=3):
    if value is None or pd.isna(value):
        return value
    return round(float(value), ndigits)

def build_weather_row(record: dict):
    observation_ts = normalize_timestamp(record.get("current_time"))
    ingested_at = normalize_timestamp(record.get("ingested_at"))
    technology = record.get("technology")

    row = {
        "observation_timestamp_utc": observation_ts,
        "event_date": observation_ts.date().isoformat() if pd.notnull(observation_ts) else None,
        "ingested_at": ingested_at,
        "source": record.get("source"),
        "dataset": record.get("dataset"),
        "country_code": record.get("country_code"),
        "country": record.get("country"),
        "bidding_zone": record.get("bidding_zone"),
        "technology": technology,
        "cluster_name": record.get("cluster_name"),
        "response_lat": record.get("response_lat"),
        "response_lon": record.get("response_lon"),
        "temperature_2m": record.get("temperature_2m"),
        "relative_humidity_2m": record.get("relative_humidity_2m"),
        "precipitation": record.get("precipitation"),
        "surface_pressure": record.get("surface_pressure"),
    }

    if technology == "solar":
        row.update({
            "cloud_cover": record.get("cloud_cover"),
            "shortwave_radiation": record.get("shortwave_radiation"),
            "direct_radiation": record.get("direct_radiation"),
            "diffuse_radiation": record.get("diffuse_radiation"),
            "global_tilted_irradiance": record.get("global_tilted_irradiance"),
            "wind_speed_100m": None,
            "wind_direction_100m": None,
            "wind_gusts_10m": None,
        })

    elif technology in ("wind-onshore", "wind-offshore"):
        row.update({
            "cloud_cover": None,
            "shortwave_radiation": None,
            "direct_radiation": None,
            "diffuse_radiation": None,
            "global_tilted_irradiance": None,
            "wind_speed_100m": record.get("wind_speed_100m"),
            "wind_direction_100m": record.get("wind_direction_100m"),
            "wind_gusts_10m": record.get("wind_gusts_10m"),
        })

    else:
        row.update({
            "cloud_cover": record.get("cloud_cover"),
            "shortwave_radiation": record.get("shortwave_radiation"),
            "direct_radiation": record.get("direct_radiation"),
            "diffuse_radiation": record.get("diffuse_radiation"),
            "global_tilted_irradiance": record.get("global_tilted_irradiance"),
            "wind_speed_100m": record.get("wind_speed_100m"),
            "wind_direction_100m": record.get("wind_direction_100m"),
            "wind_gusts_10m": record.get("wind_gusts_10m"),
        })

    for field in ROUND_FIELDS:
        row[field] = round_if_number(row.get(field), 3)

    return row


def deduplicate(df: pd.DataFrame) -> pd.DataFrame:
    key_cols = [
        "observation_timestamp_utc",
        "country_code",
        "bidding_zone",
        "technology",
        "cluster_name",
    ]

    df = df.sort_values("ingested_at").drop_duplicates(subset=key_cols, keep="last")
    return df


def write_partitioned_parquet(df: pd.DataFrame, target_bucket: str, target_prefix: str, source_key: str):
    if df.empty:
        print("No rows to write.")
        return []

    written_files = []
    run_ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    source_file_name = source_key.split("/")[-1].replace(".jsonl", "")

    for event_date, partition_df in df.groupby("event_date"):
        output_file = f"{source_file_name}_{run_ts}.parquet"
        partition_key = f"{target_prefix}/event_date={event_date}/{output_file}"

        buffer = io.BytesIO()
        partition_df.to_parquet(buffer, index=False, engine="pyarrow")
        buffer.seek(0)

        s3.put_object(
            Bucket=target_bucket,
            Key=partition_key,
            Body=buffer.getvalue(),
            ContentType="application/octet-stream",
        )

        written_uri = f"s3://{target_bucket}/{partition_key}"
        written_files.append(written_uri)
        print(f"Wrote {len(partition_df)} rows to {written_uri}")

    return written_files


def transform_file(input_bucket: str, input_key: str):
    print(f"transform_file bucket={input_bucket}")
    print(f"transform_file key={repr(input_key)}")

    try:
        meta = s3.head_object(Bucket=input_bucket, Key=input_key)
        print(f"head_object ok, size={meta['ContentLength']}")
    except ClientError as e:
        print(f"head_object failed for s3://{input_bucket}/{input_key}")
        print(f"error={e}")
        raise

    raw_records = read_jsonl_from_s3(input_bucket, input_key)
    rows = [build_weather_row(r) for r in raw_records]

    df = pd.DataFrame(rows)

    if df.empty:
        print("No data found in source file.")
        return []

    numeric_cols = [
        "response_lat",
        "response_lon",
        "temperature_2m",
        "relative_humidity_2m",
        "precipitation",
        "surface_pressure",
        "cloud_cover",
        "shortwave_radiation",
        "direct_radiation",
        "diffuse_radiation",
        "global_tilted_irradiance",
        "wind_speed_100m",
        "wind_direction_100m",
        "wind_gusts_10m",
    ]

    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df = deduplicate(df)

    return write_partitioned_parquet(df, SILVER_BUCKET, SILVER_PREFIX, input_key)


def lambda_handler(event, context):
    written_files = []

    print("EVENT:", json.dumps(event))

    for record in event.get("Records", []):
        if record.get("eventSource") != "aws:s3":
            continue

        input_bucket = record["s3"]["bucket"]["name"]
        raw_key = record["s3"]["object"]["key"]
        input_key = unquote_plus(raw_key)

        print(f"raw_key={repr(raw_key)}")
        print(f"decoded_key={repr(input_key)}")

        if not input_key.endswith(".jsonl"):
            print(f"Skipping non-jsonl file: s3://{input_bucket}/{input_key}")
            continue

        print(f"Processing s3://{input_bucket}/{input_key}")
        written = transform_file(input_bucket, input_key)
        written_files.extend(written)

    return {
        "statusCode": 200,
        "written_files": written_files,
        "count": len(written_files),
    }