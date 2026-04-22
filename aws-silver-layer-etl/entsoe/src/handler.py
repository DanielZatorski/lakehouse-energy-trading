import json
import io
import os
from datetime import datetime, timezone
from urllib.parse import unquote_plus

import boto3
from botocore.exceptions import ClientError
import pandas as pd

s3 = boto3.client("s3")

SILVER_BUCKET = os.environ["SILVER_BUCKET"]
SILVER_PREFIX = os.environ["SILVER_PREFIX"]

PROCESS_TYPE_LABELS = {
    "A01": "day_ahead",
    "A16": "realised",
    "A02": "intra_day",
    "A18": "production",
}

# Natural dedup key for ENTSOE data points:
# timestamp + bidding zone + technology (psr_type) + business type
# Covers all dataset types: psr_type is None for load/price datasets (still deduped correctly)
DEDUP_KEY_COLS = [
    "timestamp_utc",
    "area_code",
    "psr_type",
    "business_type_label",
]


def read_jsonl_from_s3(bucket: str, key: str) -> list[dict]:
    response = s3.get_object(Bucket=bucket, Key=key)
    content = response["Body"].read().decode("utf-8")
    rows = []
    for line in content.splitlines():
        line = line.strip()
        if line:
            rows.append(json.loads(line))
    return rows


def _round_or_none(value, ndigits: int):
    if value is None:
        return None
    try:
        return round(float(value), ndigits)
    except (TypeError, ValueError):
        return None


def build_entsoe_row(record: dict) -> dict | None:
    if record.get("status") != "ok":
        return None

    raw_ts = record.get("timestamp_utc")
    if not raw_ts:
        return None

    try:
        ts = pd.to_datetime(raw_ts, utc=True)
    except Exception:
        return None

    row = {
        "event_date": ts.date().isoformat(),
        "timestamp_utc": ts,
        "ingested_at": pd.to_datetime(record["ingested_at"], utc=True) if record.get("ingested_at") else None,
        "source": record.get("source"),
        "dataset": record.get("dataset"),
        "country_code": record.get("country_code"),
        "country": record.get("country"),
        "area_code": record.get("area_code"),
        "area_name": record.get("area_name"),
        "document_type": record.get("document_type") or None,
        "process_type": record.get("process_type") or None,
        "process_type_label": PROCESS_TYPE_LABELS.get(record.get("process_type") or ""),
        "period_start": record.get("period_start") or None,
        "period_end": record.get("period_end") or None,
        "resolution": record.get("resolution") or None,
        "business_type_label": record.get("business_type_label") or None,
        "psr_type": record.get("psr_type") or None,
        "technology": record.get("technology") or None,
        "in_domain": record.get("in_domain") or None,
        "out_domain": record.get("out_domain") or None,
        "value_type": record.get("value_type") or None,
        "quantity_mw": _round_or_none(record.get("quantity_mw"), 3),
        "price_amount": _round_or_none(record.get("price_amount"), 4),
        "unit": record.get("unit") or None,
        "currency_unit": record.get("currency_unit") or None,
        "price_measure_unit": record.get("price_measure_unit") or None,
    }
    return row


def deduplicate(df: pd.DataFrame) -> pd.DataFrame:
    key_cols = [c for c in DEDUP_KEY_COLS if c in df.columns]
    df = df.sort_values("ingested_at").drop_duplicates(subset=key_cols, keep="last")
    return df.reset_index(drop=True)


def write_partitioned_parquet(df: pd.DataFrame, source_key: str) -> list[str]:
    if df.empty:
        print("No rows to write.")
        return []

    run_ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    source_filename = source_key.split("/")[-1].replace(".jsonl", "")
    written_files = []

    for (event_date, dataset), group_df in df.groupby(["event_date", "dataset"]):
        output_key = (
            f"{SILVER_PREFIX}/"
            f"event_date={event_date}/"
            f"dataset={dataset}/"
            f"{source_filename}_{run_ts}.parquet"
        )

        buffer = io.BytesIO()
        group_df.to_parquet(buffer, index=False, engine="pyarrow")
        buffer.seek(0)

        s3.put_object(
            Bucket=SILVER_BUCKET,
            Key=output_key,
            Body=buffer.getvalue(),
            ContentType="application/octet-stream",
        )

        written_uri = f"s3://{SILVER_BUCKET}/{output_key}"
        written_files.append(written_uri)
        print(f"Wrote {len(group_df)} rows → {written_uri}")

    return written_files


def transform_file(input_bucket: str, input_key: str) -> list[str]:
    print(f"Processing s3://{input_bucket}/{input_key}")

    try:
        meta = s3.head_object(Bucket=input_bucket, Key=input_key)
        print(f"File size: {meta['ContentLength']} bytes")
    except ClientError as e:
        print(f"head_object failed: {e}")
        raise

    raw_records = read_jsonl_from_s3(input_bucket, input_key)
    rows = [build_entsoe_row(r) for r in raw_records]
    rows = [r for r in rows if r is not None]

    if not rows:
        print("No valid rows found — file may contain only error/no_data records.")
        return []

    df = pd.DataFrame(rows)

    for col in ["quantity_mw", "price_amount"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df = deduplicate(df)

    return write_partitioned_parquet(df, input_key)


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

        if not input_key.startswith("bronze/entsoe/"):
            print(f"Skipping non-ENTSOE key: {input_key}")
            continue

        if not input_key.endswith(".jsonl"):
            print(f"Skipping non-jsonl file: {input_key}")
            continue

        written = transform_file(input_bucket, input_key)
        written_files.extend(written)

    return {
        "statusCode": 200,
        "written_files": written_files,
        "count": len(written_files),
    }
