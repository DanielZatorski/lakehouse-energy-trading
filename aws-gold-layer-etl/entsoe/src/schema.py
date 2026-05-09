from pyiceberg.schema import Schema
from pyiceberg.types import (
    BooleanType,
    DateType,
    DoubleType,
    LongType,
    NestedField,
    StringType,
    TimestamptzType,
)

# required=False  → NOT NULL
# required=False → nullable


FACT_GENERATION = Schema(
    NestedField(1,  "event_date",            DateType(),         required=False),
    NestedField(2,  "timestamp_utc",         TimestamptzType(),  required=False),
    NestedField(3,  "country_code",          StringType(),       required=False),
    NestedField(4,  "country",               StringType(),       required=False),
    NestedField(5,  "area_code",             StringType(),       required=False),
    NestedField(6,  "area_name",             StringType(),       required=False),
    NestedField(7,  "technology",            StringType(),       required=False),
    NestedField(8,  "psr_type",              StringType(),       required=False),
    NestedField(9,  "is_renewable",          BooleanType(),      required=False),
    NestedField(10, "process_type_label",    StringType(),       required=False),
    NestedField(11, "business_type_label",   StringType(),       required=False),
    NestedField(12, "period_start",          TimestamptzType(),  required=False),
    NestedField(13, "period_end",            TimestamptzType(),  required=False),
    NestedField(14, "resolution",            StringType(),       required=False),
    NestedField(15, "quantity_mw",           DoubleType(),       required=False),
    NestedField(16, "unit",                  StringType(),       required=False),
    NestedField(17, "source",                StringType(),       required=False),
    NestedField(18, "ingested_at",           TimestamptzType(),  required=False),
)

FACT_LOAD = Schema(
    NestedField(1,  "event_date",            DateType(),         required=False),
    NestedField(2,  "timestamp_utc",         TimestamptzType(),  required=False),
    NestedField(3,  "country_code",          StringType(),       required=False),
    NestedField(4,  "country",               StringType(),       required=False),
    NestedField(5,  "area_code",             StringType(),       required=False),
    NestedField(6,  "area_name",             StringType(),       required=False),
    NestedField(7,  "dataset",               StringType(),       required=False),
    NestedField(8,  "process_type_label",    StringType(),       required=False),
    NestedField(9,  "period_start",          TimestamptzType(),  required=False),
    NestedField(10, "period_end",            TimestamptzType(),  required=False),
    NestedField(11, "resolution",            StringType(),       required=False),
    NestedField(12, "quantity_mw",           DoubleType(),       required=False),
    NestedField(13, "unit",                  StringType(),       required=False),
    NestedField(14, "source",                StringType(),       required=False),
    NestedField(15, "ingested_at",           TimestamptzType(),  required=False),
)

FACT_PRICES = Schema(
    NestedField(1,  "event_date",            DateType(),         required=False),
    NestedField(2,  "timestamp_utc",         TimestamptzType(),  required=False),
    NestedField(3,  "country_code",          StringType(),       required=False),
    NestedField(4,  "country",               StringType(),       required=False),
    NestedField(5,  "area_code",             StringType(),       required=False),
    NestedField(6,  "area_name",             StringType(),       required=False),
    NestedField(7,  "period_start",          TimestamptzType(),  required=False),
    NestedField(8,  "period_end",            TimestamptzType(),  required=False),
    NestedField(9,  "resolution",            StringType(),       required=False),
    NestedField(10, "price_amount",          DoubleType(),       required=False),
    NestedField(11, "currency_unit",         StringType(),       required=False),
    NestedField(12, "price_measure_unit",    StringType(),       required=False),
    NestedField(13, "source",                StringType(),       required=False),
    NestedField(14, "ingested_at",           TimestamptzType(),  required=False),
)

FACT_GENERATION_FORECAST = Schema(
    NestedField(1,  "event_date",            DateType(),         required=False),
    NestedField(2,  "timestamp_utc",         TimestamptzType(),  required=False),
    NestedField(3,  "country_code",          StringType(),       required=False),
    NestedField(4,  "country",               StringType(),       required=False),
    NestedField(5,  "area_code",             StringType(),       required=False),
    NestedField(6,  "area_name",             StringType(),       required=False),
    NestedField(7,  "dataset",               StringType(),       required=False),
    NestedField(8,  "technology",            StringType(),       required=False),
    NestedField(9,  "psr_type",              StringType(),       required=False),
    NestedField(10, "business_type_label",   StringType(),       required=False),
    NestedField(11, "process_type_label",    StringType(),       required=False),
    NestedField(12, "period_start",          TimestamptzType(),  required=False),
    NestedField(13, "period_end",            TimestamptzType(),  required=False),
    NestedField(14, "resolution",            StringType(),       required=False),
    NestedField(15, "quantity_mw",           DoubleType(),       required=False),
    NestedField(16, "unit",                  StringType(),       required=False),
    NestedField(17, "source",                StringType(),       required=False),
    NestedField(18, "ingested_at",           TimestamptzType(),  required=False),
)

AGG_DAILY_GENERATION_MIX = Schema(
    NestedField(1,  "event_date",                DateType(),    required=False),
    NestedField(2,  "country_code",              StringType(),  required=False),
    NestedField(3,  "country",                   StringType(),  required=False),
    NestedField(4,  "area_code",                 StringType(),  required=False),
    NestedField(5,  "area_name",                 StringType(),  required=False),
    NestedField(6,  "technology",                StringType(),  required=False),
    NestedField(7,  "psr_type",                  StringType(),  required=False),
    NestedField(8,  "is_renewable",              BooleanType(), required=False),
    NestedField(9,  "total_mwh",                 DoubleType(),  required=False),
    NestedField(10, "avg_mw",                    DoubleType(),  required=False),
    NestedField(11, "peak_mw",                   DoubleType(),  required=False),
    NestedField(12, "min_mw",                    DoubleType(),  required=False),
    NestedField(13, "observation_count",         LongType(),    required=False),
    NestedField(14, "area_renewable_share_pct",  DoubleType(),  required=False),
)

# Map used by write_iceberg_table to look up the schema for a given table name
SCHEMAS = {
    "fact_generation":           FACT_GENERATION,
    "fact_load":                 FACT_LOAD,
    "fact_prices":               FACT_PRICES,
    "fact_generation_forecast":  FACT_GENERATION_FORECAST,
    "agg_daily_generation_mix":  AGG_DAILY_GENERATION_MIX,
}
