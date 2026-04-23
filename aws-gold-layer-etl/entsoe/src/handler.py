import io
import json
import os
from datetime import datetime, timedelta, timezone

import boto3
import pandas as pd

s3 = boto3.client("s3")

SILVER_BUCKET = os.environ["SILVER_BUCKET"]
SILVER_PREFIX = os.environ["SILVER_PREFIX"]   # silver/entsoe
GOLD_BUCKET = os.environ["GOLD_BUCKET"]
GOLD_PREFIX = os.environ["GOLD_PREFIX"]       # gold/entsoe

RENEWABLE_TECHNOLOGIES = {
    "solar",
    "wind_onshore",
    "wind_offshore",
    "hydro_run_of_river",
    "hydro_reservoir",
    "hydro_pumped_storage",
    "geothermal",
    "biomass",
    "other_renewable",
}

GENERATION_DATASETS   = {"actual_generation_per_production_type"}
LOAD_DATASETS         = {"actual_total_load", "day_ahead_total_load_forecast"}
PRICE_DATASETS        = {"day_ahead_energy_prices"}
FORECAST_DATASETS     = {"day_ahead_aggregated_generation", "generation_forecast_wind_solar"}


# ---------------------------------------------------------------------------
# Read helpers
# ---------------------------------------------------------------------------

def read_silver_dataset(event_date: str, dataset: str) -> pd.DataFrame:
    prefix = f"{SILVER_PREFIX}/event_date={event_date}/dataset={dataset}/"
    response = s3.list_objects_v2(Bucket=SILVER_BUCKET, Prefix=prefix)
    objects = response.get("Contents", [])

    if not objects:
        print(f"No files at s3://{SILVER_BUCKET}/{prefix}")
        return pd.DataFrame()

    frames = []
    for obj in objects:
        key = obj["Key"]
        if not key.endswith(".parquet"):
            continue
        body = s3.get_object(Bucket=SILVER_BUCKET, Key=key)["Body"].read()
        df = pd.read_parquet(io.BytesIO(body), engine="pyarrow")
        frames.append(df)
        print(f"Read {len(df)} rows from {key}")

    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def read_silver_multi(event_date: str, datasets: set) -> pd.DataFrame:
    frames = [read_silver_dataset(event_date, d) for d in sorted(datasets)]
    frames = [f for f in frames if not f.empty]
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


# ---------------------------------------------------------------------------
# Table builders
# ---------------------------------------------------------------------------

def build_fact_generation(df: pd.DataFrame) -> pd.DataFrame:
    """Actual generation per production type — one row per (timestamp, area, technology)."""
    if df.empty:
        return df

    key_cols = ["timestamp_utc", "area_code", "psr_type"]
    df = (
        df.sort_values("ingested_at")
          .drop_duplicates(subset=key_cols, keep="last")
    )

    # Negative MW is physically impossible for generation readings
    if "quantity_mw" in df.columns:
        df.loc[df["quantity_mw"] < 0, "quantity_mw"] = pd.NA

    df["is_renewable"] = df["technology"].isin(RENEWABLE_TECHNOLOGIES)

    col_order = [
        "event_date", "timestamp_utc",
        "country_code", "country", "area_code", "area_name",
        "technology", "psr_type", "is_renewable",
        "process_type_label", "business_type_label",
        "period_start", "period_end", "resolution",
        "quantity_mw", "unit",
        "source", "ingested_at",
    ]
    return df[[c for c in col_order if c in df.columns]].reset_index(drop=True)


def build_fact_load(df: pd.DataFrame) -> pd.DataFrame:
    """Actual and forecast total load — one row per (timestamp, area, dataset)."""
    if df.empty:
        return df

    key_cols = ["timestamp_utc", "area_code", "dataset"]
    df = (
        df.sort_values("ingested_at")
          .drop_duplicates(subset=key_cols, keep="last")
    )

    # Zero or negative load is physically implausible
    if "quantity_mw" in df.columns:
        df.loc[df["quantity_mw"] <= 0, "quantity_mw"] = pd.NA

    col_order = [
        "event_date", "timestamp_utc",
        "country_code", "country", "area_code", "area_name",
        "dataset", "process_type_label",
        "period_start", "period_end", "resolution",
        "quantity_mw", "unit",
        "source", "ingested_at",
    ]
    return df[[c for c in col_order if c in df.columns]].reset_index(drop=True)


def build_fact_prices(df: pd.DataFrame) -> pd.DataFrame:
    """Day-ahead energy prices — one row per (timestamp, area)."""
    if df.empty:
        return df

    key_cols = ["timestamp_utc", "area_code"]
    df = (
        df.sort_values("ingested_at")
          .drop_duplicates(subset=key_cols, keep="last")
    )

    # Drop rows with no price value — they carry no information
    df = df[df["price_amount"].notna()].copy()

    col_order = [
        "event_date", "timestamp_utc",
        "country_code", "country", "area_code", "area_name",
        "period_start", "period_end", "resolution",
        "price_amount", "currency_unit", "price_measure_unit",
        "source", "ingested_at",
    ]
    return df[[c for c in col_order if c in df.columns]].reset_index(drop=True)


def build_fact_generation_forecast(df: pd.DataFrame) -> pd.DataFrame:
    """Day-ahead generation forecast + wind/solar forecast."""
    if df.empty:
        return df

    possible_keys = ["timestamp_utc", "area_code", "psr_type", "business_type_label", "dataset"]
    key_cols = [c for c in possible_keys if c in df.columns]
    df = (
        df.sort_values("ingested_at")
          .drop_duplicates(subset=key_cols, keep="last")
    )

    if "quantity_mw" in df.columns:
        df.loc[df["quantity_mw"] < 0, "quantity_mw"] = pd.NA

    col_order = [
        "event_date", "timestamp_utc",
        "country_code", "country", "area_code", "area_name",
        "dataset", "technology", "psr_type",
        "business_type_label", "process_type_label",
        "period_start", "period_end", "resolution",
        "quantity_mw", "unit",
        "source", "ingested_at",
    ]
    return df[[c for c in col_order if c in df.columns]].reset_index(drop=True)


def build_agg_daily_generation_mix(fact_gen: pd.DataFrame, event_date: str) -> pd.DataFrame:
    """Daily generation mix per (area, technology) with area-level renewable share."""
    if fact_gen.empty:
        return fact_gen

    group_cols = [
        "country_code", "country",
        "area_code", "area_name",
        "technology", "psr_type", "is_renewable",
    ]

    # Guard: only aggregate over rows that have a quantity
    gen = fact_gen[fact_gen["quantity_mw"].notna()].copy()
    if gen.empty:
        return pd.DataFrame()

    agg = (
        gen.groupby(group_cols, dropna=False)
        .agg(
            total_mwh         =("quantity_mw", "sum"),   # hourly data → MWh per hour summed
            avg_mw            =("quantity_mw", "mean"),
            peak_mw           =("quantity_mw", "max"),
            min_mw            =("quantity_mw", "min"),
            observation_count =("quantity_mw", "count"),
        )
        .reset_index()
    )

    # Area-level totals for renewable share calculation
    area_total = agg.groupby("area_code")["total_mwh"].sum().rename("_area_total")
    area_renewable = (
        agg[agg["is_renewable"]]
        .groupby("area_code")["total_mwh"]
        .sum()
        .rename("_area_renewable")
    )
    agg = agg.join(area_total, on="area_code").join(area_renewable, on="area_code")

    agg["area_renewable_share_pct"] = (
        (agg["_area_renewable"] / agg["_area_total"] * 100)
        .where(agg["_area_total"] > 0)
        .round(2)
    )
    agg = agg.drop(columns=["_area_total", "_area_renewable"])

    agg["event_date"] = event_date

    float_cols = ["total_mwh", "avg_mw", "peak_mw", "min_mw"]
    agg[float_cols] = agg[float_cols].round(3)

    col_order = [
        "event_date", "country_code", "country",
        "area_code", "area_name",
        "technology", "psr_type", "is_renewable",
        "total_mwh", "avg_mw", "peak_mw", "min_mw",
        "observation_count", "area_renewable_share_pct",
    ]
    return agg[[c for c in col_order if c in agg.columns]].reset_index(drop=True)


# ---------------------------------------------------------------------------
# Write helper
# ---------------------------------------------------------------------------

def write_gold_parquet(df: pd.DataFrame, table_name: str, event_date: str) -> str:
    if df.empty:
        print(f"No data to write for '{table_name}'")
        return ""

    run_ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    key = f"{GOLD_PREFIX}/{table_name}/event_date={event_date}/{table_name}_{run_ts}.parquet"

    buf = io.BytesIO()
    df.to_parquet(buf, index=False, engine="pyarrow")
    buf.seek(0)

    s3.put_object(
        Bucket=GOLD_BUCKET,
        Key=key,
        Body=buf.getvalue(),
        ContentType="application/octet-stream",
    )

    uri = f"s3://{GOLD_BUCKET}/{key}"
    print(f"Wrote {len(df)} rows → {uri}")
    return uri


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

def process_date(event_date: str) -> dict:
    print(f"Gold ENTSOE ETL — event_date={event_date}")

    written = []
    counts = {}

    # --- actual generation -----------------------------------------------
    gen_raw = read_silver_multi(event_date, GENERATION_DATASETS)
    fact_gen = build_fact_generation(gen_raw)
    uri = write_gold_parquet(fact_gen, "fact_generation", event_date)
    if uri:
        written.append(uri)
    counts["fact_generation"] = len(fact_gen)

    # --- daily generation mix (derived from fact_generation) -------------
    agg_mix = build_agg_daily_generation_mix(fact_gen, event_date)
    uri = write_gold_parquet(agg_mix, "agg_daily_generation_mix", event_date)
    if uri:
        written.append(uri)
    counts["agg_daily_generation_mix"] = len(agg_mix)

    # --- load (actual + day-ahead forecast) ------------------------------
    load_raw = read_silver_multi(event_date, LOAD_DATASETS)
    fact_load = build_fact_load(load_raw)
    uri = write_gold_parquet(fact_load, "fact_load", event_date)
    if uri:
        written.append(uri)
    counts["fact_load"] = len(fact_load)

    # --- day-ahead prices ------------------------------------------------
    price_raw = read_silver_multi(event_date, PRICE_DATASETS)
    fact_prices = build_fact_prices(price_raw)
    uri = write_gold_parquet(fact_prices, "fact_prices", event_date)
    if uri:
        written.append(uri)
    counts["fact_prices"] = len(fact_prices)

    # --- generation forecast (day-ahead + wind/solar) --------------------
    forecast_raw = read_silver_multi(event_date, FORECAST_DATASETS)
    fact_forecast = build_fact_generation_forecast(forecast_raw)
    uri = write_gold_parquet(fact_forecast, "fact_generation_forecast", event_date)
    if uri:
        written.append(uri)
    counts["fact_generation_forecast"] = len(fact_forecast)

    return {"event_date": event_date, "written_files": written, "row_counts": counts}


def lambda_handler(event, context):
    print("EVENT:", json.dumps(event))

    if "date" in event:
        event_date = event["date"]
    else:
        yesterday = datetime.now(timezone.utc).date() - timedelta(days=1)
        event_date = yesterday.isoformat()

    result = process_date(event_date)
    return {"statusCode": 200, **result}
