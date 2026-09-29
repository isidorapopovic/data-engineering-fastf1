CREATE OR REPLACE TEMP VIEW bronze_laps
USING PARQUET
OPTIONS (
    path 'data/bronze/season=2025/round=*/laps.parquet'
);

CREATE OR REPLACE TEMP VIEW standardized_laps AS
SELECT
    CAST(2025 AS INT) AS season,
    CAST(round AS INT) AS round,
    UPPER(TRIM(Driver)) AS driver_code,
    CAST(DriverNumber AS INT) AS driver_number,
    LOWER(TRIM(Team)) AS team_name,
    CAST(LapNumber AS INT) AS lap_number,
    CAST(Stint AS INT) AS stint,
    CAST(Position AS INT) AS position,
    UPPER(TRIM(Compound)) AS tyre_compound,
    CAST(TyreLife AS DOUBLE) AS tyre_life,
    CAST(FreshTyre AS BOOLEAN) AS fresh_tyre,
    CAST(LapTime AS BIGINT) AS lap_time_raw,
    CAST(Sector1Time AS BIGINT) AS sector1_time_raw,
    CAST(Sector2Time AS BIGINT) AS sector2_time_raw,
    CAST(Sector3Time AS BIGINT) AS sector3_time_raw,
    CAST(PitInTime AS BIGINT) AS pit_in_time,
    CAST(PitOutTime AS BIGINT) AS pit_out_time,
    CAST(SpeedI1 AS DOUBLE) AS speed_i1,
    CAST(SpeedI2 AS DOUBLE) AS speed_i2,
    CAST(SpeedFL AS DOUBLE) AS speed_finish_line,
    CAST(SpeedST AS DOUBLE) AS speed_trap,
    CAST(TrackStatus AS STRING) AS track_status,
    CAST(LapStartDate AS TIMESTAMP_NTZ) AS lap_start_timestamp,
    CAST(IsPersonalBest AS BOOLEAN) AS is_personal_best,
    DeletedReason AS deleted_reason,
    CAST(FastF1Generated AS BOOLEAN) AS is_fastf1_generated,
    CAST(IsAccurate AS BOOLEAN) AS is_accurate,

    CASE
        WHEN Deleted = 1 THEN TRUE
        WHEN Deleted = 0 THEN FALSE
        ELSE NULL
    END AS is_deleted

FROM bronze_laps;

CREATE OR REPLACE TEMP VIEW flagged_laps AS
SELECT *,
CASE WHEN pit_in_time IS NOT NULL
OR pit_out_time IS NOT  NULL
THEN TRUE 
ELSE FALSE END as is_pit_lap,

CASE WHEN lap_number IS NOT NULL
AND driver_code IS NOT NULL 
AND lap_time_raw IS NOT NULL
AND (is_deleted IS NULL OR is_deleted = FALSE)
AND (is_accurate IS NULL OR is_accurate=FALSE)
THEN TRUE 
ELSE FALSE
END AS is_valid_lap

from standardized_laps;


CREATE OR REPLACE TEMP VIEW filtered_laps AS
SELECT * 
FROM flagged_laps
WHERE driver_code IN NOT NULL
AND lap_number >0 
AND LENGTH (driver_code)>0
AND (position is NULL OR position >0);

CREATE OR REPLACE TEMP VIEW normalized_laps AS 
SELECT season,round,driver_code,driver_number,team_name, lap_number,stint,position
 CASE
        WHEN tyre_compound IN (
            'SOFT',
            'MEDIUM',
            'HARD',
            'INTERMEDIATE'
        )
        THEN tyre_compound

        ELSE 'UNKNOWN'
    END AS tyre_compound,
      tyre_life,
    fresh_tyre,

    lap_time_raw,

    sector1_time_raw,
    sector2_time_raw,
    sector3_time_raw,

    pit_in_time,
    pit_out_time,

    speed_i1,
    speed_i2,
    speed_finish_line,
    speed_trap,

    track_status,
    lap_start_timestamp,

    is_personal_best,
    deleted_reason,
    is_fastf1_generated,

    is_accurate,
    is_deleted,

    is_pit_lap,
    is_valid_lap

FROM filtered_laps;


CREATE OR REPLACE TEMP VIEW silver_laps AS
WITH ranked_laps AS(
    SELECT *,
    ROW_NUMBER() OVER (
        PARTITION BY 
        season,round, driver_code, lap_number
        ORDER BY is_accurate DESC, is_deleted ASC NULLS LAST
    ) as row_number
    FROM normalized_laps
)
SELECT

    season,
    round,

    driver_code,
    driver_number,
    team_name,

    lap_number,
    stint,
    position,

    tyre_compound,
    tyre_life,
    fresh_tyre,

    lap_time_raw,

    sector1_time_raw,
    sector2_time_raw,
    sector3_time_raw,

    pit_in_time,
    pit_out_time,

    speed_i1,
    speed_i2,
    speed_finish_line,
    speed_trap,

    track_status,
    lap_start_timestamp,

    is_personal_best,
    deleted_reason,
    is_fastf1_generated,

    is_accurate,
    is_deleted,

    is_pit_lap,
    is_valid_lap

FROM ranked_laps

WHERE row_number = 1;

SELECT COUNT(*) AS silver_row_count
FROM silver_laps;



SELECT

    season,
    round,
    driver_code,
    lap_number,

    COUNT(*) AS duplicate_count

FROM silver_laps

GROUP BY

    season,
    round,
    driver_code,
    lap_number

HAVING COUNT(*) > 1;


CREATE OR REPLACE TABLE silver.laps
USING DELTA
PARTITIONED BY (
    season,
    round
)

AS

SELECT *
FROM silver_laps;