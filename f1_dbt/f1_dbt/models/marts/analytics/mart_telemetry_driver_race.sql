
{{ config(materialized='table') }}

with laps as (

    select *
    from {{ ref('mart_telemetry_lap') }}

)

select

    season,
    round,
    session,
    driver_code,
    driver_key,
    event_key,

    count(distinct lap_number) as laps_with_telemetry,

    sum(telemetry_samples) as total_telemetry_samples,

    round(
        sum(avg_speed_kmh * telemetry_samples)
        / nullif(sum(telemetry_samples), 0),
        2
    ) as sample_weighted_avg_speed_kmh,

    max(max_speed_kmh) as max_speed_kmh,

    round(avg(avg_rpm), 2) as avg_lap_mean_rpm,

    round(avg(avg_throttle_pct), 2) as avg_lap_throttle_pct,

    round(avg(brake_sample_pct), 2) as avg_lap_brake_sample_pct,

    sum(drs_active_samples) as total_drs_active_samples

from laps

group by
    season,
    round,
    session,
    driver_code,
    driver_key,
    event_key
