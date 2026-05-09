# Energy Trading Lakehouse

A production-grade data lakehouse on AWS that collects real-time European electricity market data and weather observations, cleans them, and makes them queryable via SQL in Athena.

Built on the **medallion architecture** — data flows through three layers: Bronze → Silver → Gold.

---

## What it does

Every hour, the system automatically:
1. Fetches electricity generation, load, and price data from ENTSOE (the European grid operator) across 14 countries
2. Fetches weather observations (temperature, wind, solar radiation, pressure) from Open-Meteo for energy-relevant locations across the same countries
3. Cleans and structures the data
4. Writes it to queryable Iceberg tables in AWS Athena

The result is a SQL-queryable dataset covering the relationship between weather conditions and electricity generation, load, and prices across Europe.

---

## Architecture

```
                    ┌─────────────────────────────────────────┐
                    │              Data Sources                │
                    │  ENTSOE API          Open-Meteo API      │
                    │  (electricity grid)  (weather)           │
                    └────────────┬─────────────┬──────────────┘
                                 │             │
                           every hour    every hour
                                 │             │
                    ┌────────────▼─────────────▼──────────────┐
                    │         BRONZE LAYER  (S3)               │
                    │   Raw JSON files, one file per run       │
                    │   bronze/entsoe/event_date=YYYY-MM-DD/   │
                    │   bronze/weather/event_date=YYYY-MM-DD/  │
                    └────────────┬─────────────┬──────────────┘
                                 │             │
                    S3 event triggers automatically
                                 │             │
                    ┌────────────▼─────────────▼──────────────┐
                    │         SILVER LAYER  (S3)               │
                    │   Cleaned Parquet files                  │
                    │   silver/entsoe/event_date=YYYY-MM-DD/   │
                    │   silver/weather_current/event_date=.../ │
                    └────────────┬─────────────┬──────────────┘
                                 │             │
                           daily at 04:00 UTC
                                 │             │
                    ┌────────────▼─────────────▼──────────────┐
                    │          GOLD LAYER  (Iceberg)           │
                    │   Structured tables, queryable in Athena │
                    │   fact_generation   fact_prices          │
                    │   fact_load         fact_hourly_weather  │
                    │   agg_daily_generation_mix  ...          │
                    └─────────────────────────────────────────┘
                                       │
                                  Athena SQL
```

---

## The Three Layers Explained

### Bronze — Raw data, never modified

The bronze layer stores exactly what the APIs return, converted to JSONL (one JSON record per line). Nothing is changed or filtered. If an API returns bad data, it is preserved here so it can be investigated and reprocessed later.

- Stored in: `s3://data-lake-energy-trade/bronze/`
- Format: JSONL
- Triggered: hourly via AWS EventBridge schedule

### Silver — Cleaned and structured

When a bronze file lands in S3, it automatically triggers a Lambda that:
- Parses and validates the raw JSON
- Deduplicates records
- Standardises timestamps to UTC
- Adds country codes, area names, and technology labels
- Writes the result as Parquet (a columnar format that is much faster to query than JSON)

Silver is not directly queried — it is an intermediate step used by the gold layer.

- Stored in: `s3://data-lake-energy-trade/silver/`
- Format: Parquet, partitioned by `event_date=YYYY-MM-DD`
- Triggered: automatically by S3 event when bronze file arrives

### Gold — Analysis-ready Iceberg tables

Every day at 04:00 UTC, the gold Lambdas read the silver Parquet files, apply final business logic (renewable energy flags, daily aggregations, MWh calculations), and write the results as Apache Iceberg tables.

**ENTSOE tables (electricity grid data):**

| Table | What it contains |
|---|---|
| `fact_generation` | Hourly actual power generation per technology per bidding zone |
| `fact_load` | Hourly actual and forecast electricity demand |
| `fact_prices` | Hourly day-ahead electricity prices |
| `fact_generation_forecast` | Day-ahead wind and solar generation forecasts |
| `agg_daily_generation_mix` | Daily MWh totals and renewable share % per area |

**Weather tables:**

| Table | What it contains |
|---|---|
| `fact_hourly_weather` | Hourly weather observations per location cluster |
| `agg_daily_bidding_zone` | Daily weather summary per bidding zone and energy technology |

All tables are queryable directly in AWS Athena under the `energy_gold` Glue database.

---

## What is Apache Iceberg?

Iceberg is a **metadata layer** that sits on top of Parquet files in S3. It does not move or change the files — it just keeps track of them.

Think of it like a library catalogue. The books (Parquet files) sit on the shelves (S3). Iceberg is the catalogue that tells you which books exist, where they are, and what is inside them. When Athena runs a query, it reads the catalogue first, finds only the relevant files, and reads just those — instead of scanning everything.

This gives you three things plain Parquet cannot:
- **Partition pruning** — a query for `event_date = '2026-05-08'` reads only that day's files
- **Idempotent writes** — rerunning the pipeline for the same date safely replaces the old data in a single atomic operation, no duplicates
- **Schema evolution** — columns can be added or renamed without rewriting existing files

The table schemas are explicitly defined in `schema.py` in each gold Lambda. This means column types are declared upfront rather than guessed from data at runtime.

---

## Data Coverage

- **Countries:** Germany, France, Netherlands, Belgium, Spain, Portugal, Italy, Poland, Czech Republic, Austria, Switzerland, Denmark, Norway, Sweden (14 total)
- **Electricity data:** ENTSOE Transparency Platform
- **Weather data:** Open-Meteo (observation clusters per bidding zone and energy technology type)
- **History:** data accumulates daily from when the pipeline was first deployed

---

## Repository Structure

```
lakehouse-energy-trading/
│
├── aws-bronze-layer-lambda/
│   ├── nordpool/          ENTSOE electricity data ingestion Lambda
│   └── openweather/       Weather data ingestion Lambda
│
├── aws-silver-layer-etl/
│   ├── entsoe/            Parses and cleans ENTSOE bronze data
│   └── openweather/       Parses and cleans weather bronze data
│
├── aws-gold-layer-etl/
│   ├── entsoe/            Builds 5 Iceberg fact/agg tables for electricity
│   └── openweather/       Builds 2 Iceberg fact/agg tables for weather
│
├── aws-data-catalog/
│   └── template.yml       CloudFormation — Glue databases + Athena workgroup
│
├── airflow/
│   ├── docker-compose.yml Local Airflow setup (runs in Docker)
│   └── dags/
│       └── entsoe_pipeline.py  DAG that orchestrates bronze → silver → gold
│
└── athena_queries.sql     Pre-written SQL queries for common analyses
```

Each Lambda folder follows the same structure:
```
lambda-name/
├── src/
│   ├── handler.py         Lambda entry point
│   ├── schema.py          Iceberg table schema definitions (gold only)
│   ├── Dockerfile         Container image definition
│   └── requirements.txt   Python dependencies
├── template.yml           SAM/CloudFormation resource definition
└── samconfig.toml         Deployment configuration (region, stack name)
```

---

## How Data Flows Step by Step

```
1. EventBridge fires at :30 past every hour
         ↓
2. Bronze Lambda runs → calls ENTSOE API and Open-Meteo API
         ↓
3. Raw responses written as JSONL to S3 bronze prefix
         ↓
4. S3 event notification fires automatically
         ↓
5. Silver Lambda triggered → reads JSONL, cleans, writes Parquet
         ↓
6. EventBridge fires at 04:00 UTC next day
         ↓
7. Gold Lambda runs → reads silver Parquet, builds Iceberg tables
         ↓
8. Tables available in Athena under energy_gold database
```

---

## Backfilling Historical Data

Because silver Parquet files accumulate automatically every hour as long as the bronze and silver Lambdas are running, the gold layer can be backfilled for any past date at any time — the data is already in S3 waiting to be processed.

**How it works:**

The gold Lambda accepts an optional `{"date": "YYYY-MM-DD"}` payload. When you invoke it for a past date, it reads that date's silver partition and writes the Iceberg table partition for that date — exactly the same as a normal daily run, just pointed at history.

```
silver/entsoe/event_date=2026-04-01/*.parquet  ← already exists
        ↓  gold Lambda invoked with {"date": "2026-04-01"}
gold/entsoe/iceberg/fact_generation/data/event_date=2026-04-01/*.parquet  ← written now
```

A new Iceberg snapshot is created that includes the new partition alongside all existing ones. Athena sees the updated metadata immediately — the historical data becomes queryable as soon as the Lambda finishes.

**Backfill a date range via AWS CLI:**

```bash
# Backfill data snapshots for both gold Lambdas
for d in $(seq -w 1 30); do
  aws lambda invoke \
    --function-name data-lake-energy-trade-gold-entsoe \
    --payload "{\"date\": \"2026-04-$d\"}" \
    --invocation-type Event \
    --cli-binary-format raw-in-base64-out /dev/null
done
```

Using `--invocation-type Event` fires all invocations asynchronously so you don't wait for each one. All dates process in parallel on AWS.

**Reruns are safe:** `overwrite()` replaces only the partition for the specified date — it never touches other dates. Running the same date twice produces the same result.

---

## Local Orchestration with Airflow

For manual runs or backfilling, Apache Airflow runs locally in Docker and mirrors the same flow:

```bash
cd airflow
docker compose up
```

Open `http://localhost:8080` (admin / admin), enable the `entsoe_pipeline` DAG and trigger it manually. The DAG:
1. Invokes the bronze Lambdas
2. Waits (using S3KeySensor) until silver files appear in S3
3. Invokes the gold Lambdas with the date payload

---

## Deployment

Each Lambda is packaged as a Docker image and deployed via AWS SAM:

```bash
cd aws-gold-layer-etl/entsoe
sam build
sam deploy
```

Repeat for each Lambda folder (`aws-bronze-layer-lambda/nordpool`, etc.).

The `aws-data-catalog` stack only needs to be deployed once:
```bash
cd aws-data-catalog
aws cloudformation deploy --template-file template.yml --stack-name energy-data-catalog
```

---

## Querying the Data

Open the **Athena console**, select the `energy-trading` workgroup and `energy_gold` database.

Example — daily renewable energy share by country:
```sql
SELECT
    event_date,
    country,
    ROUND(AVG(area_renewable_share_pct), 1) AS renewable_share_pct
FROM fact_generation
WHERE event_date >= DATE '2026-05-01'
GROUP BY event_date, country
ORDER BY event_date, renewable_share_pct DESC;
```

Pre-written queries for generation mix, price analysis, solar and wind performance, and weather correlation are available in `athena_queries.sql`.

---

## Tech Stack

| Layer | Technology |
|---|---|
| Compute | AWS Lambda (Docker image, Python 3.13) |
| Storage | Amazon S3 |
| Table format | Apache Iceberg via PyIceberg |
| Catalog | AWS Glue Data Catalog |
| Query engine | AWS Athena |
| Infrastructure | AWS SAM / CloudFormation |
| Orchestration | Apache Airflow (local Docker) |
| Data processing | pandas, PyArrow |
| Scheduling | AWS EventBridge |
