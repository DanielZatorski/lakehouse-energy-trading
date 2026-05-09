import io
import json
import os
from datetime import date, datetime, timedelta, timezone

import boto3
import pandas as pd
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.expressions import EqualTo
from schema import SCHEMAS

s3 = boto3.client("s3")

SILVER_BUCKET = os.environ["SILVER_BUCKET"]
SILVER_PREFIX = os.environ["SILVER_PREFIX"]   # silver/weather_current
GOLD_BUCKET   = os.environ["GOLD_BUCKET"]
GOLD_PREFIX   = os.environ["GOLD_PREFIX"]     # gold/weather
GLUE_DATABASE = os.environ.get("GLUE_DATABASE", "energy_gold")
AWS_REGION    = os.environ.get("AWS_REGION", "eu-central-1")

catalog = load_catalog("glue", **{
    "type": "glue",
    "region_name": AWS_REGION,
    "io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
})

# Physical plausibility bounds — values outside these windows are set to NULL
# rather than silently poisoning downstream aggregations.
VALID_RANGES = {
    "temperature_2m":           (-60.0,  60.0),
    "relative_humidity_2m":     (  0.0, 100.0),
    "precipitation":            (  0.0, 500.0),
    "surface_pressure":         (850.0, 1100.0),
    "cloud_cover":              (  0.0, 100.0),
    "shortwave_radiation":      (  0.0, 1500.0),
    "direct_radiation":         (  0.0, 1500.0),
    "diffuse_radiation":        (  0.0, 1000.0),
    "global_tilted_irradiance": (  0.0, 1500.0),
    "wind_speed_100m":          (  0.0,  120.0),
    "wind_direction_100m":      (  0.0,  360.0),
    "wind_gusts_10m":           (  0.0,  200.0),
}

RENEWABLE_TECHNOLOGIES = {"solar", "wind-onshore", "wind-offshore"}

# Columns that must be UTC-aware timestamps in Iceberg
TS_COLS = frozenset({"observation_timestamp_utc", "ingested_at"})


def read_silver_partition(event_date: str) -> pd.DataFrame:
    prefix = f"{SILVER_PREFIX}/event_date={event_date}/"
    paginator = s3.get_paginator("list_objects_v2")
    objects = []
    for page in paginator.paginate(Bucket=SILVER_BUCKET, Prefix=prefix):
        objects.extend(page.get("Contents", []))

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


def clamp_to_valid_ranges(df: pd.DataFrame) -> pd.DataFrame:
    for col, (lo, hi) in VALID_RANGES.items():
        if col not in df.columns:
            continue
        mask = df[col].notna() & ~df[col].between(lo, hi, inclusive="both")
        if mask.any():
            print(f"Nulling {mask.sum()} out-of-range values in '{col}'")
            df.loc[mask, col] = pd.NA
    return df


def build_fact_hourly_weather(raw: pd.DataFrame) -> pd.DataFrame:
    if raw.empty:
        return raw

    # Keep the most-recently-ingested record per natural observation key
    key_cols = ["observation_timestamp_utc", "cluster_name"]
    df = (
        raw.sort_values("ingested_at")
           .drop_duplicates(subset=key_cols, keep="last")
    )

    df = clamp_to_valid_ranges(df)

    df["technology_group"] = df["technology"].apply(
        lambda t: "wind" if t in ("wind-onshore", "wind-offshore") else t
    )
    df["is_renewable"] = df["technology"].isin(RENEWABLE_TECHNOLOGIES)

    col_order = [
        "event_date",
        "observation_timestamp_utc",
        "country_code",
        "country",
        "bidding_zone",
        "technology",
        "technology_group",
        "is_renewable",
        "cluster_name",
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
        "source",
        "ingested_at",
    ]
    existing = [c for c in col_order if c in df.columns]
    return df[existing].reset_index(drop=True)


def _safe_sum_to_kwh(series: pd.Series) -> float | None:
    """Sum W/m² hourly readings → approximate kWh/m² (1 obs = 1 h × power)."""
    valid = series.dropna()
    if valid.empty:
        return None
    return round(float(valid.sum()) / 1000, 3)


def _safe_quantile(series: pd.Series, q: float) -> float | None:
    valid = series.dropna()
    if valid.empty:
        return None
    return round(float(valid.quantile(q)), 3)


def build_agg_daily_bidding_zone(fact: pd.DataFrame) -> pd.DataFrame:
    """Daily summary per (bidding_zone, technology) — one row per group."""
    if fact.empty:
        return fact

    group_cols = [
        "event_date", "country_code", "country",
        "bidding_zone", "technology", "technology_group", "is_renewable",
    ]

    agg = (
        fact.groupby(group_cols, dropna=False)
        .agg(
            observation_count           =("observation_timestamp_utc", "count"),
            temperature_2m_avg          =("temperature_2m", "mean"),
            temperature_2m_min          =("temperature_2m", "min"),
            temperature_2m_max          =("temperature_2m", "max"),
            relative_humidity_avg       =("relative_humidity_2m", "mean"),
            precipitation_sum_mm        =("precipitation", "sum"),
            surface_pressure_avg        =("surface_pressure", "mean"),
            cloud_cover_avg             =("cloud_cover", "mean"),
            shortwave_radiation_avg     =("shortwave_radiation", "mean"),
            direct_radiation_avg        =("direct_radiation", "mean"),
            diffuse_radiation_avg       =("diffuse_radiation", "mean"),
            gti_avg                     =("global_tilted_irradiance", "mean"),
            wind_speed_100m_avg         =("wind_speed_100m", "mean"),
            wind_speed_100m_max         =("wind_speed_100m", "max"),
            wind_gusts_10m_max          =("wind_gusts_10m", "max"),
        )
        .reset_index()
    )

    # Derived energy columns require the raw series — compute separately
    def per_group_kwh(col_name: str, agg_col_name: str) -> pd.Series:
        return (
            fact.groupby(group_cols, dropna=False)[col_name]
            .apply(_safe_sum_to_kwh)
            .rename(agg_col_name)
            .reset_index()
        )

    for raw_col, agg_col in [
        ("shortwave_radiation", "shortwave_daily_kwh_m2"),
        ("global_tilted_irradiance", "gti_daily_kwh_m2"),
    ]:
        kwh_series = per_group_kwh(raw_col, agg_col)
        agg = agg.merge(kwh_series, on=group_cols, how="left")

    # P90 wind speed — informative for capacity factor estimation
    p90_series = (
        fact.groupby(group_cols, dropna=False)["wind_speed_100m"]
        .apply(lambda x: _safe_quantile(x, 0.9))
        .rename("wind_speed_100m_p90")
        .reset_index()
    )
    agg = agg.merge(p90_series, on=group_cols, how="left")

    float_cols = agg.select_dtypes("float64").columns
    agg[float_cols] = agg[float_cols].round(3)

    return agg


# ---------------------------------------------------------------------------
# Iceberg write helpers
# ---------------------------------------------------------------------------

def _to_arrow(df: pd.DataFrame) -> pa.Table:
    """Fix event_date (string → date32) and timestamp columns (naive → UTC) for Iceberg."""
    df = df.copy()

    if "event_date" in df.columns:
        df["event_date"] = pd.to_datetime(df["event_date"]).dt.date

    for col in TS_COLS & set(df.columns):
        if not pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = pd.to_datetime(df[col], utc=True)
        elif df[col].dt.tz is None:
            df[col] = df[col].dt.tz_localize("UTC")

    table = pa.Table.from_pandas(df, preserve_index=False)

    # Iceberg v2 rejects pa.null() — cast all-null columns to string
    for i, field in enumerate(table.schema):
        if pa.types.is_null(field.type):
            table = table.set_column(i, field.name, table.column(i).cast(pa.string()))

    return table


def write_iceberg_table(df: pd.DataFrame, table_name: str, event_date: str) -> str:
    if df.empty:
        print(f"No data to write for '{table_name}'")
        return ""

    pa_table = _to_arrow(df)
    identifier = f"{GLUE_DATABASE}.{table_name}"
    location = f"s3://{GOLD_BUCKET}/{GOLD_PREFIX}/iceberg/{table_name}"

    try:
        tbl = catalog.load_table(identifier)
    except NoSuchTableError:
        explicit_schema = SCHEMAS[table_name]
        tbl = catalog.create_table(identifier, schema=explicit_schema, location=location)
        with tbl.update_spec() as update:
            update.add_identity("event_date")
        print(f"Created Iceberg table with explicit schema: {identifier}")

    # Single atomic snapshot — no data-loss window between delete and append
    tbl.overwrite(pa_table, overwrite_filter=EqualTo("event_date", date.fromisoformat(event_date)))

    print(f"Wrote {len(df)} rows → iceberg:{identifier} (event_date={event_date})")
    return identifier


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

def process_date(event_date: str) -> dict:
    print(f"Gold weather ETL — event_date={event_date}")

    raw = read_silver_partition(event_date)
    if raw.empty:
        return {"event_date": event_date, "written_tables": [], "row_counts": {}}

    fact = build_fact_hourly_weather(raw)
    agg = build_agg_daily_bidding_zone(fact)

    written = []
    counts = {}

    for df, table in [(fact, "fact_hourly_weather"), (agg, "agg_daily_bidding_zone")]:
        tbl = write_iceberg_table(df, table, event_date)
        if tbl:
            written.append(tbl)
        counts[table] = len(df)

    return {"event_date": event_date, "written_tables": written, "row_counts": counts}


def lambda_handler(event, context):
    print("EVENT:", json.dumps(event))

    if "date" in event:
        event_date = event["date"]
    else:
        yesterday = datetime.now(timezone.utc).date() - timedelta(days=1)
        event_date = yesterday.isoformat()

    result = process_date(event_date)
    return {"statusCode": 200, **result}
