CREATE OR REPLACE TEMP VIEW bronze_weather
USING PARQUET
OPTIONS (
    path 'data/bronze/season=2025/round=*/weather.parquet'
);


CREATE OR REPLACE TEMP VIEW standardized_weather AS
SELECT
    CAST(2025 AS INT) AS season,
    CAST(round AS INT) AS round,
    CAST(Time AS BIGINT) AS time_raw_ns,
    CAST(Time AS DOUBLE) / 1000000000.0 AS session_time_seconds,
    CAST(AirTemp AS DOUBLE) AS air_temp,
    CAST(Humidity AS DOUBLE) AS humidity,
    CAST(Pressure AS DOUBLE) AS pressure,
    CAST(Rainfall AS BOOLEAN) AS rainfall,
    CAST(TrackTemp AS DOUBLE) AS track_temp,
    CAST(WindDirection AS INT) AS wind_direction,
    CAST(WindSpeed AS DOUBLE) AS wind_speed
FROM bronze_weather;

CREATE OR REPLACE TEMP VIEW cleaned_weather AS
SELECT
    season,
    round,
    time_raw_ns,
    session_time_seconds,
    air_temp,
    CASE
        WHEN humidity BETWEEN 0 AND 100
        THEN humidity
        ELSE NULL
    END AS humidity,
    pressure,
    rainfall,
    track_temp,
    CASE
        WHEN wind_direction IS NOT NULL
        THEN PMOD(wind_direction, 360)
        ELSE NULL
    END AS wind_direction,
    CASE
        WHEN wind_speed >= 0
        THEN wind_speed
        ELSE NULL
    END AS wind_speed

FROM standardized_weather;


CREATE OR REPLACE TEMP VIEW enriched_weather AS
SELECT  *,
    ROUND(
        track_temp - air_temp,
        2
    ) AS track_air_temp_delta,
    CASE
        WHEN rainfall = TRUE
        THEN 'WET'
        WHEN rainfall = FALSE
        THEN 'DRY'
        ELSE 'UNKNOWN'
    END AS weather_condition
FROM cleaned_weather;



CREATE OR REPLACE TEMP VIEW deduplicated_weather AS

WITH ranked_weather AS (
    SELECT  *,
        ROW_NUMBER() OVER (
            PARTITION BY
                season,
                round,
                time_raw_ns
            ORDER BY
                time_raw_ns
        ) AS row_number
    FROM enriched_weather
)
SELECT
    season,
    round,
    time_raw_ns,
    session_time_seconds,
    air_temp,
    humidity,
    pressure,
    rainfall,
    track_temp,
    wind_direction,
    wind_speed,
    track_air_temp_delta,
    weather_condition

FROM ranked_weather
WHERE row_number = 1;


CREATE OR REPLACE TEMP VIEW silver_weather AS
SELECT *,
    CASE
        WHEN session_time_seconds IS NOT NULL
         AND air_temp IS NOT NULL
         AND track_temp IS NOT NULL
         AND humidity BETWEEN 0 AND 100
         AND wind_speed >= 0
        THEN TRUE
        ELSE FALSE
    END AS is_valid_weather
FROM deduplicated_weather;

SELECT COUNT(*) AS silver_weather_row_count
FROM silver_weather;

SELECT

    SUM(
        CASE
            WHEN session_time_seconds IS NULL THEN 1
            ELSE 0
        END
    ) AS session_time_seconds_nulls,

    SUM(
        CASE
            WHEN air_temp IS NULL THEN 1
            ELSE 0
        END
    ) AS air_temp_nulls,

    SUM(
        CASE
            WHEN humidity IS NULL THEN 1
            ELSE 0
        END
    ) AS humidity_nulls,

    SUM(
        CASE
            WHEN pressure IS NULL THEN 1
            ELSE 0
        END
    ) AS pressure_nulls,

    SUM(
        CASE
            WHEN track_temp IS NULL THEN 1
            ELSE 0
        END
    ) AS track_temp_nulls,

    SUM(
        CASE
            WHEN wind_speed IS NULL THEN 1
            ELSE 0
        END
    ) AS wind_speed_nulls

FROM silver_weather;

SELECT

    season,
    round,
    time_raw_ns,

    COUNT(*) AS row_count

FROM silver_weather

GROUP BY

    season,
    round,
    time_raw_ns

HAVING COUNT(*) > 1;





