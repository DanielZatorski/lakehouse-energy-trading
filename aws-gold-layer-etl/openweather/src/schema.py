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


FACT_HOURLY_WEATHER = Schema(
    NestedField(1,  "event_date",                  DateType(),         required=False),
    NestedField(2,  "observation_timestamp_utc",    TimestamptzType(),  required=False),
    NestedField(3,  "country_code",                StringType(),       required=False),
    NestedField(4,  "country",                     StringType(),       required=False),
    NestedField(5,  "bidding_zone",                StringType(),       required=False),
    NestedField(6,  "technology",                  StringType(),       required=False),
    NestedField(7,  "technology_group",            StringType(),       required=False),
    NestedField(8,  "is_renewable",                BooleanType(),      required=False),
    NestedField(9,  "cluster_name",                StringType(),       required=False),
    NestedField(10, "response_lat",                DoubleType(),       required=False),
    NestedField(11, "response_lon",                DoubleType(),       required=False),
    NestedField(12, "temperature_2m",              DoubleType(),       required=False),
    NestedField(13, "relative_humidity_2m",        DoubleType(),       required=False),
    NestedField(14, "precipitation",               DoubleType(),       required=False),
    NestedField(15, "surface_pressure",            DoubleType(),       required=False),
    NestedField(16, "cloud_cover",                 DoubleType(),       required=False),
    NestedField(17, "shortwave_radiation",         DoubleType(),       required=False),
    NestedField(18, "direct_radiation",            DoubleType(),       required=False),
    NestedField(19, "diffuse_radiation",           DoubleType(),       required=False),
    NestedField(20, "global_tilted_irradiance",    DoubleType(),       required=False),
    NestedField(21, "wind_speed_100m",             DoubleType(),       required=False),
    NestedField(22, "wind_direction_100m",         DoubleType(),       required=False),
    NestedField(23, "wind_gusts_10m",              DoubleType(),       required=False),
    NestedField(24, "source",                      StringType(),       required=False),
    NestedField(25, "ingested_at",                 TimestamptzType(),  required=False),
)

AGG_DAILY_BIDDING_ZONE = Schema(
    NestedField(1,  "event_date",                  DateType(),    required=False),
    NestedField(2,  "country_code",                StringType(),  required=False),
    NestedField(3,  "country",                     StringType(),  required=False),
    NestedField(4,  "bidding_zone",                StringType(),  required=False),
    NestedField(5,  "technology",                  StringType(),  required=False),
    NestedField(6,  "technology_group",            StringType(),  required=False),
    NestedField(7,  "is_renewable",                BooleanType(), required=False),
    NestedField(8,  "observation_count",           LongType(),    required=False),
    NestedField(9,  "temperature_2m_avg",          DoubleType(),  required=False),
    NestedField(10, "temperature_2m_min",          DoubleType(),  required=False),
    NestedField(11, "temperature_2m_max",          DoubleType(),  required=False),
    NestedField(12, "relative_humidity_avg",       DoubleType(),  required=False),
    NestedField(13, "precipitation_sum_mm",        DoubleType(),  required=False),
    NestedField(14, "surface_pressure_avg",        DoubleType(),  required=False),
    NestedField(15, "cloud_cover_avg",             DoubleType(),  required=False),
    NestedField(16, "shortwave_radiation_avg",     DoubleType(),  required=False),
    NestedField(17, "direct_radiation_avg",        DoubleType(),  required=False),
    NestedField(18, "diffuse_radiation_avg",       DoubleType(),  required=False),
    NestedField(19, "gti_avg",                     DoubleType(),  required=False),
    NestedField(20, "wind_speed_100m_avg",         DoubleType(),  required=False),
    NestedField(21, "wind_speed_100m_max",         DoubleType(),  required=False),
    NestedField(22, "wind_gusts_10m_max",          DoubleType(),  required=False),
    NestedField(23, "shortwave_daily_kwh_m2",      DoubleType(),  required=False),
    NestedField(24, "gti_daily_kwh_m2",            DoubleType(),  required=False),
    NestedField(25, "wind_speed_100m_p90",         DoubleType(),  required=False),
)

# Map used by write_iceberg_table to look up the schema for a given table name
SCHEMAS = {
    "fact_hourly_weather":     FACT_HOURLY_WEATHER,
    "agg_daily_bidding_zone":  AGG_DAILY_BIDDING_ZONE,
}
