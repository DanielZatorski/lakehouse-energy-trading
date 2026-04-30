-- =============================================================================
-- Energy Trading Lakehouse — Athena Query Library
-- Database:  energy_gold
-- Workgroup: energy-trading
-- =============================================================================
-- Tables
--   fact_generation            hourly actual generation per area / technology
--   fact_load                  hourly actual + forecast load per area
--   fact_prices                hourly day-ahead prices per area
--   fact_generation_forecast   hourly wind/solar + aggregated generation forecasts
--   agg_daily_generation_mix   daily generation totals + renewable share per area
--   fact_hourly_weather        hourly weather at 22 renewable energy sites
--   agg_daily_bidding_zone     daily weather aggregates per bidding zone
-- =============================================================================
-- Join key notes:
--   fact_hourly_weather.bidding_zone  = "DE-LU", "IT-North"  (human-readable)
--   fact_generation.area_name         = "DE-LU", "IT-North"  (same values — use this)
--   fact_generation.area_code         = "10Y1001A1001A82H"   (EIC — DO NOT join to bidding_zone)
-- =============================================================================


-- =============================================================================
-- 0. SANITY CHECKS
-- =============================================================================

-- Row counts across all tables
SELECT 'fact_generation'           AS tbl, COUNT(*) AS rows FROM energy_gold.fact_generation
UNION ALL
SELECT 'fact_load',                         COUNT(*) FROM energy_gold.fact_load
UNION ALL
SELECT 'fact_generation_forecast',          COUNT(*) FROM energy_gold.fact_generation_forecast
UNION ALL
SELECT 'agg_daily_generation_mix',          COUNT(*) FROM energy_gold.agg_daily_generation_mix
UNION ALL
SELECT 'fact_hourly_weather',               COUNT(*) FROM energy_gold.fact_hourly_weather
UNION ALL
SELECT 'agg_daily_bidding_zone',            COUNT(*) FROM energy_gold.agg_daily_bidding_zone;

-- Area code → human-readable name mapping
SELECT DISTINCT
    area_code,
    area_name,
    country_code,
    country
FROM energy_gold.fact_generation
ORDER BY country_code, area_code;


-- =============================================================================
-- 1. VIEWS
-- =============================================================================

-- 1a. Hourly supply/demand balance per area
CREATE OR REPLACE VIEW energy_gold.v_supply_demand AS
SELECT
    g.event_date,
    g.area_code,
    g.area_name,
    g.country_code,
    g.country,
    g.timestamp_utc,
    SUM(g.quantity_mw)                                                  AS total_generation_mw,
    SUM(CASE WHEN g.is_renewable THEN g.quantity_mw END)                AS renewable_mw,
    l.quantity_mw                                                       AS total_load_mw,
    SUM(g.quantity_mw) - l.quantity_mw                                  AS surplus_mw
FROM energy_gold.fact_generation g
JOIN energy_gold.fact_load l
    ON  g.area_code     = l.area_code
    AND g.timestamp_utc = l.timestamp_utc
    AND l.dataset       = 'actual_total_load'
GROUP BY g.event_date, g.area_code, g.area_name, g.country_code, g.country,
         g.timestamp_utc, l.quantity_mw;

-- 1b. Daily renewable share by country
CREATE OR REPLACE VIEW energy_gold.v_daily_renewable_share AS
SELECT
    event_date,
    country_code,
    area_code,
    SUM(total_mwh)                                                      AS total_mwh,
    SUM(CASE WHEN is_renewable THEN total_mwh END)                      AS renewable_mwh,
    MAX(area_renewable_share_pct)                                       AS renewable_share_pct
FROM energy_gold.agg_daily_generation_mix
GROUP BY event_date, country_code, area_code;

-- 1c. Daily solar performance — generation vs irradiance
CREATE OR REPLACE VIEW energy_gold.v_solar_performance AS
SELECT
    w.event_date,
    w.country_code,
    w.bidding_zone,
    w.shortwave_daily_kwh_m2,
    w.gti_daily_kwh_m2,
    w.cloud_cover_avg,
    w.shortwave_radiation_avg,
    SUM(g.quantity_mw)                                                          AS total_solar_mwh,
    MAX(g.quantity_mw)                                                          AS peak_solar_mw,
    ROUND(MAX(g.quantity_mw) / NULLIF(w.shortwave_radiation_avg, 0), 4)         AS peak_mw_per_avg_wm2
FROM energy_gold.agg_daily_bidding_zone w
LEFT JOIN energy_gold.fact_generation g
    ON  w.country_code = g.country_code
    AND w.event_date   = g.event_date
    AND w.bidding_zone = g.area_name
    AND g.technology   = 'solar'
WHERE w.technology = 'solar'
GROUP BY w.event_date, w.country_code, w.bidding_zone,
         w.shortwave_daily_kwh_m2, w.gti_daily_kwh_m2,
         w.cloud_cover_avg, w.shortwave_radiation_avg;

-- 1d. Daily wind performance — generation vs wind speed
CREATE OR REPLACE VIEW energy_gold.v_wind_performance AS
SELECT
    w.event_date,
    w.country_code,
    w.bidding_zone,
    w.technology,
    w.wind_speed_100m_avg,
    w.wind_speed_100m_max,
    w.wind_speed_100m_p90,
    SUM(g.quantity_mw)                                                  AS total_wind_mwh,
    MAX(g.quantity_mw)                                                  AS peak_wind_mw
FROM energy_gold.agg_daily_bidding_zone w
LEFT JOIN energy_gold.fact_generation g
    ON  w.country_code = g.country_code
    AND w.event_date   = g.event_date
    AND w.bidding_zone = g.area_name
    AND g.technology   IN ('wind_onshore', 'wind_offshore')
WHERE w.technology IN ('wind-onshore', 'wind-offshore')
GROUP BY w.event_date, w.country_code, w.bidding_zone, w.technology,
         w.wind_speed_100m_avg, w.wind_speed_100m_max, w.wind_speed_100m_p90;

-- 1e. Area code lookup
CREATE OR REPLACE VIEW energy_gold.v_area_lookup AS
SELECT DISTINCT
    area_code,
    area_name,
    country_code,
    country
FROM energy_gold.fact_generation;


-- =============================================================================
-- 2. SOLAR: GENERATION VS IRRADIATION
-- =============================================================================

-- Q1. Hourly solar generation vs irradiation per bidding zone
SELECT
    w.event_date,
    w.bidding_zone,
    w.cluster_name,
    w.observation_timestamp_utc,
    w.global_tilted_irradiance                                         AS gti_wm2,
    w.shortwave_radiation,
    w.cloud_cover,
    g.quantity_mw                                                      AS solar_mw
FROM energy_gold.fact_hourly_weather w
LEFT JOIN energy_gold.fact_generation g
    ON  w.bidding_zone              = g.area_name
    AND w.observation_timestamp_utc = g.timestamp_utc
    AND g.technology                = 'solar'
WHERE w.technology = 'solar'
  AND w.event_date = DATE '2026-04-22'
ORDER BY w.bidding_zone, w.observation_timestamp_utc;

-- Q2. Daily solar generation vs daily irradiation — which zones convert best
SELECT
    w.event_date,
    w.country_code,
    w.bidding_zone,
    w.gti_daily_kwh_m2,
    w.shortwave_daily_kwh_m2,
    w.cloud_cover_avg,
    g.total_mwh                                                        AS solar_total_mwh,
    g.peak_mw                                                          AS solar_peak_mw,
    ROUND(g.total_mwh / NULLIF(w.gti_daily_kwh_m2, 0), 4)             AS mwh_per_kwh_m2
FROM energy_gold.agg_daily_bidding_zone w
LEFT JOIN energy_gold.agg_daily_generation_mix g
    ON  w.country_code = g.country_code
    AND w.event_date   = g.event_date
    AND w.bidding_zone = g.area_name
    AND g.technology   = 'solar'
WHERE w.technology = 'solar'
ORDER BY mwh_per_kwh_m2 DESC NULLS LAST;

-- Q3. Solar correlation: generation vs irradiation per bidding zone
SELECT
    w.bidding_zone,
    COUNT(*)                                                           AS hours,
    ROUND(CORR(w.global_tilted_irradiance, g.quantity_mw), 4)         AS corr_gti,
    ROUND(CORR(w.shortwave_radiation,      g.quantity_mw), 4)         AS corr_shortwave,
    ROUND(CORR(w.cloud_cover,             g.quantity_mw), 4)         AS corr_cloud_cover
FROM energy_gold.fact_hourly_weather w
LEFT JOIN energy_gold.fact_generation g
    ON  w.bidding_zone              = g.area_name
    AND w.observation_timestamp_utc = g.timestamp_utc
    AND g.technology                = 'solar'
WHERE w.technology              = 'solar'
  AND w.global_tilted_irradiance IS NOT NULL
  AND g.quantity_mw              IS NOT NULL
GROUP BY w.bidding_zone
HAVING COUNT(*) >= 10
ORDER BY corr_gti DESC;

-- Q7. Solar generation by irradiation bucket
SELECT
    w.bidding_zone,
    CASE
        WHEN w.global_tilted_irradiance <  50  THEN '0-50   W/m²  very low'
        WHEN w.global_tilted_irradiance < 200  THEN '50-200 W/m²  low'
        WHEN w.global_tilted_irradiance < 500  THEN '200-500 W/m² moderate'
        WHEN w.global_tilted_irradiance < 800  THEN '500-800 W/m² high'
        ELSE                                        '800+   W/m²  peak'
    END                                                                AS irradiance_bucket,
    COUNT(*)                                                           AS hours,
    ROUND(AVG(g.quantity_mw), 2)                                      AS avg_mw,
    ROUND(MAX(g.quantity_mw), 2)                                      AS peak_mw
FROM energy_gold.fact_hourly_weather w
LEFT JOIN energy_gold.fact_generation g
    ON  w.bidding_zone              = g.area_name
    AND w.observation_timestamp_utc = g.timestamp_utc
    AND g.technology                = 'solar'
WHERE w.technology              = 'solar'
  AND w.global_tilted_irradiance IS NOT NULL
GROUP BY w.bidding_zone, 2
ORDER BY w.bidding_zone, irradiance_bucket;


-- =============================================================================
-- 3. WIND: GENERATION VS WIND SPEED
-- =============================================================================

-- Q4. Hourly wind generation vs wind speed and gusts
SELECT
    w.event_date,
    w.bidding_zone,
    w.technology,
    w.observation_timestamp_utc,
    w.wind_speed_100m,
    w.wind_gusts_10m,
    w.wind_direction_100m,
    g.quantity_mw                                                      AS wind_mw
FROM energy_gold.fact_hourly_weather w
LEFT JOIN energy_gold.fact_generation g
    ON  w.bidding_zone              = g.area_name
    AND w.observation_timestamp_utc = g.timestamp_utc
    AND g.technology                IN ('wind_onshore', 'wind_offshore')
WHERE w.technology IN ('wind-onshore', 'wind-offshore')
  AND w.event_date  = DATE '2026-04-22'
ORDER BY w.bidding_zone, w.observation_timestamp_utc;

-- Q5. Wind correlation: generation vs wind speed and gusts
SELECT
    w.bidding_zone,
    w.technology,
    COUNT(*)                                                           AS hours,
    ROUND(CORR(w.wind_speed_100m, g.quantity_mw), 4)                  AS corr_wind_speed,
    ROUND(CORR(w.wind_gusts_10m,  g.quantity_mw), 4)                  AS corr_gusts,
    ROUND(AVG(w.wind_speed_100m), 2)                                  AS avg_wind_speed,
    ROUND(AVG(g.quantity_mw), 2)                                      AS avg_generation_mw
FROM energy_gold.fact_hourly_weather w
LEFT JOIN energy_gold.fact_generation g
    ON  w.bidding_zone              = g.area_name
    AND w.observation_timestamp_utc = g.timestamp_utc
    AND g.technology                IN ('wind_onshore', 'wind_offshore')
WHERE w.technology     IN ('wind-onshore', 'wind-offshore')
  AND w.wind_speed_100m IS NOT NULL
  AND g.quantity_mw     IS NOT NULL
GROUP BY w.bidding_zone, w.technology
HAVING COUNT(*) >= 10
ORDER BY corr_wind_speed DESC;

-- Q6. Wind generation by wind-speed bucket
SELECT
    w.bidding_zone,
    w.technology,
    CASE
        WHEN w.wind_speed_100m <  5 THEN '00-05 m/s  calm'
        WHEN w.wind_speed_100m < 10 THEN '05-10 m/s  light'
        WHEN w.wind_speed_100m < 15 THEN '10-15 m/s  moderate'
        WHEN w.wind_speed_100m < 20 THEN '15-20 m/s  strong'
        WHEN w.wind_speed_100m < 25 THEN '20-25 m/s  very strong'
        ELSE                             '25+   m/s  storm'
    END                                                                AS wind_bucket,
    COUNT(*)                                                           AS hours,
    ROUND(AVG(g.quantity_mw), 2)                                      AS avg_mw,
    ROUND(MAX(g.quantity_mw), 2)                                      AS peak_mw,
    ROUND(MIN(g.quantity_mw), 2)                                      AS min_mw
FROM energy_gold.fact_hourly_weather w
LEFT JOIN energy_gold.fact_generation g
    ON  w.bidding_zone              = g.area_name
    AND w.observation_timestamp_utc = g.timestamp_utc
    AND g.technology                IN ('wind_onshore', 'wind_offshore')
WHERE w.technology     IN ('wind-onshore', 'wind-offshore')
  AND w.wind_speed_100m IS NOT NULL
GROUP BY w.bidding_zone, w.technology, 3
ORDER BY w.bidding_zone, wind_bucket;


-- =============================================================================
-- 4. BENCHMARKING & NORMALIZATION
-- =============================================================================

-- Q8. Normalize generation — compare zones against their own historical peak
SELECT
    event_date,
    area_code,
    country_code,
    technology,
    avg_mw,
    peak_mw,
    MAX(peak_mw) OVER (PARTITION BY area_code, technology)             AS zone_hist_peak,
    ROUND(avg_mw  / NULLIF(MAX(peak_mw) OVER (PARTITION BY area_code, technology), 0), 4) AS normalized_avg,
    ROUND(peak_mw / NULLIF(MAX(peak_mw) OVER (PARTITION BY area_code, technology), 0), 4) AS normalized_peak
FROM energy_gold.agg_daily_generation_mix
WHERE technology IN ('solar', 'wind_onshore', 'wind_offshore')
ORDER BY event_date, area_code, technology;

-- Q9. Daily benchmark — best renewable response to weather conditions
SELECT
    g.event_date,
    g.country_code,
    g.technology,
    g.total_mwh,
    g.area_renewable_share_pct,
    w.gti_daily_kwh_m2,
    w.wind_speed_100m_avg,
    w.wind_speed_100m_p90,
    CASE
        WHEN g.technology = 'solar'
        THEN ROUND(g.total_mwh / NULLIF(w.gti_daily_kwh_m2,    0), 3)
        ELSE ROUND(g.total_mwh / NULLIF(w.wind_speed_100m_avg, 0), 3)
    END                                                                AS weather_adjusted_score
FROM energy_gold.agg_daily_generation_mix g
LEFT JOIN energy_gold.agg_daily_bidding_zone w
    ON  g.country_code = w.country_code
    AND g.event_date   = w.event_date
    AND g.area_name    = w.bidding_zone
    AND (
            (g.technology = 'solar'                            AND w.technology = 'solar')
         OR (g.technology IN ('wind_onshore','wind_offshore')  AND w.technology IN ('wind-onshore','wind-offshore'))
    )
WHERE g.technology IN ('solar', 'wind_onshore', 'wind_offshore')
ORDER BY weather_adjusted_score DESC NULLS LAST;


-- =============================================================================
-- 5. FORECAST VS ACTUAL
-- =============================================================================

-- Q10. Actual generation vs forecast — hourly error
SELECT
    a.event_date,
    a.area_code,
    a.country_code,
    a.technology,
    a.timestamp_utc,
    a.quantity_mw                                                              AS actual_mw,
    f.quantity_mw                                                              AS forecast_mw,
    a.quantity_mw - f.quantity_mw                                              AS error_mw,
    ROUND(ABS(a.quantity_mw - f.quantity_mw) / NULLIF(a.quantity_mw, 0) * 100, 2) AS abs_pct_error
FROM energy_gold.fact_generation a
JOIN energy_gold.fact_generation_forecast f
    ON  a.area_code     = f.area_code
    AND a.timestamp_utc = f.timestamp_utc
    AND a.technology    = f.technology
WHERE a.technology IN ('solar', 'wind_onshore', 'wind_offshore')
  AND a.event_date  = DATE '2026-04-22'
  AND a.quantity_mw IS NOT NULL
  AND f.quantity_mw IS NOT NULL
ORDER BY abs_pct_error DESC;

-- Q11. Forecast error vs weather intensity — do extreme conditions cause bigger errors
SELECT
    a.area_code,
    a.technology,
    w.wind_speed_100m,
    w.wind_gusts_10m,
    w.cloud_cover,
    w.global_tilted_irradiance,
    a.quantity_mw - f.quantity_mw                                      AS error_mw,
    ABS(a.quantity_mw - f.quantity_mw)                                 AS abs_error_mw
FROM energy_gold.fact_generation a
JOIN energy_gold.fact_generation_forecast f
    ON  a.area_code     = f.area_code
    AND a.timestamp_utc = f.timestamp_utc
    AND a.technology    = f.technology
JOIN energy_gold.fact_hourly_weather w
    ON  a.area_name     = w.bidding_zone
    AND a.timestamp_utc = w.observation_timestamp_utc
    AND (
            (a.technology = 'solar'                            AND w.technology = 'solar')
         OR (a.technology IN ('wind_onshore','wind_offshore')  AND w.technology IN ('wind-onshore','wind-offshore'))
    )
WHERE a.technology IN ('solar', 'wind_onshore', 'wind_offshore')
  AND a.quantity_mw IS NOT NULL
  AND f.quantity_mw IS NOT NULL
ORDER BY abs_error_mw DESC;


-- =============================================================================
-- 6. LOAD COVERAGE
-- =============================================================================

-- Q12. Renewable generation vs load — hourly wind+solar coverage percentage
SELECT
    g.event_date,
    g.area_code,
    g.area_name,
    g.country_code,
    g.timestamp_utc,
    SUM(g.quantity_mw)                                                                           AS total_generation_mw,
    SUM(CASE WHEN g.technology IN ('solar','wind_onshore','wind_offshore') THEN g.quantity_mw END) AS wind_solar_mw,
    SUM(CASE WHEN g.is_renewable THEN g.quantity_mw END)                                         AS total_renewable_mw,
    l.quantity_mw                                                                                AS load_mw,
    ROUND(SUM(CASE WHEN g.technology IN ('solar','wind_onshore','wind_offshore') THEN g.quantity_mw END)
          / NULLIF(l.quantity_mw, 0) * 100, 2)                                                   AS wind_solar_coverage_pct,
    ROUND(SUM(CASE WHEN g.is_renewable THEN g.quantity_mw END)
          / NULLIF(l.quantity_mw, 0) * 100, 2)                                                   AS renewable_coverage_pct
FROM energy_gold.fact_generation g
JOIN energy_gold.fact_load l
    ON  g.area_code     = l.area_code
    AND g.timestamp_utc = l.timestamp_utc
    AND l.dataset       = 'actual_total_load'
WHERE g.event_date = DATE '2026-04-22'
GROUP BY g.event_date, g.area_code, g.area_name, g.country_code, g.timestamp_utc, l.quantity_mw
ORDER BY g.area_code, g.timestamp_utc;


-- =============================================================================
-- 7. ANOMALY DETECTION
-- =============================================================================

-- Q13. Best and worst solar days — generation relative to available irradiance
WITH daily AS (
    SELECT
        g.event_date,
        g.country_code,
        SUM(g.total_mwh)                                                       AS solar_mwh,
        AVG(w.gti_daily_kwh_m2)                                                AS avg_gti,
        ROUND(SUM(g.total_mwh) / NULLIF(AVG(w.gti_daily_kwh_m2), 0), 4)       AS efficiency
    FROM energy_gold.agg_daily_generation_mix g
    JOIN energy_gold.agg_daily_bidding_zone w
        ON  g.country_code = w.country_code
        AND g.event_date   = w.event_date
        AND g.area_name    = w.bidding_zone
        AND w.technology   = 'solar'
    WHERE g.technology       = 'solar'
      AND w.gti_daily_kwh_m2 > 0
    GROUP BY g.event_date, g.country_code
)
SELECT
    event_date,
    country_code,
    solar_mwh,
    avg_gti,
    efficiency,
    ROUND(AVG(efficiency) OVER (PARTITION BY country_code), 4)                 AS country_avg_efficiency,
    ROUND(efficiency - AVG(efficiency) OVER (PARTITION BY country_code), 4)    AS vs_avg
FROM daily
ORDER BY vs_avg ASC;

-- Q14. Wind underperformance days — strong wind but weak generation
WITH daily AS (
    SELECT
        g.event_date,
        g.country_code,
        SUM(g.total_mwh)                                                           AS wind_mwh,
        AVG(w.wind_speed_100m_avg)                                                 AS avg_wind_speed,
        AVG(w.wind_speed_100m_p90)                                                 AS p90_wind_speed,
        ROUND(SUM(g.total_mwh) / NULLIF(AVG(w.wind_speed_100m_avg), 0), 4)        AS mwh_per_ms
    FROM energy_gold.agg_daily_generation_mix g
    JOIN energy_gold.agg_daily_bidding_zone w
        ON  g.country_code = w.country_code
        AND g.event_date   = w.event_date
        AND g.area_name    = w.bidding_zone
        AND w.technology   IN ('wind-onshore', 'wind-offshore')
    WHERE g.technology         IN ('wind_onshore', 'wind_offshore')
      AND w.wind_speed_100m_avg > 5
    GROUP BY g.event_date, g.country_code
)
SELECT
    event_date,
    country_code,
    wind_mwh,
    avg_wind_speed,
    p90_wind_speed,
    mwh_per_ms,
    ROUND(AVG(mwh_per_ms) OVER (PARTITION BY country_code), 4)                    AS avg_mwh_per_ms,
    ROUND(mwh_per_ms / NULLIF(AVG(mwh_per_ms) OVER (PARTITION BY country_code), 0) - 1, 4) AS performance_vs_avg
FROM daily
ORDER BY performance_vs_avg ASC;


-- =============================================================================
-- 8. MONTHLY BENCHMARK
-- =============================================================================

-- Q15. Monthly weather-generation benchmark per country and technology
SELECT
    DATE_TRUNC('month', CAST(g.event_date AS TIMESTAMP))               AS month,
    g.country_code,
    g.technology,
    SUM(g.total_mwh)                                                   AS total_mwh,
    ROUND(AVG(g.avg_mw), 2)                                           AS avg_mw,
    CASE
        WHEN g.technology = 'solar'
        THEN ROUND(AVG(w.gti_daily_kwh_m2), 3)
        ELSE ROUND(AVG(w.wind_speed_100m_avg), 3)
    END                                                                AS avg_weather_intensity,
    CASE
        WHEN g.technology = 'solar'
        THEN ROUND(SUM(g.total_mwh) / NULLIF(SUM(w.gti_daily_kwh_m2),      0), 4)
        ELSE ROUND(SUM(g.total_mwh) / NULLIF(SUM(w.wind_speed_100m_avg),   0), 4)
    END                                                                AS monthly_performance_score
FROM energy_gold.agg_daily_generation_mix g
LEFT JOIN energy_gold.agg_daily_bidding_zone w
    ON  g.country_code = w.country_code
    AND g.event_date   = w.event_date
    AND g.area_name    = w.bidding_zone
    AND (
            (g.technology = 'solar'                            AND w.technology = 'solar')
         OR (g.technology IN ('wind_onshore','wind_offshore')  AND w.technology IN ('wind-onshore','wind-offshore'))
    )
WHERE g.technology IN ('solar', 'wind_onshore', 'wind_offshore')
GROUP BY 1, g.country_code, g.technology
ORDER BY month, monthly_performance_score DESC NULLS LAST;
