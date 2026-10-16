
{{ config(materialized='table') }}

with telemetry as (

    select *
    from {{ ref('fact_telemetry') }}

),

lap_stats as (

    select
        season,
        round,
        session,
        driver_code,
        driver_key,
        event_key,
        lap_number,

        count(*) as telemetry_samples,

        round(avg(speed_kmh), 2) as avg_speed_kmh,
        round(max(speed_kmh), 2) as max_speed_kmh,
        round(min(speed_kmh), 2) as min_speed_kmh,

        round(avg(rpm), 2) as avg_rpm,
        max(rpm) as max_rpm,

        round(avg(throttle_pct), 2) as avg_throttle_pct,

        round(
            100.0 * avg(
                case
                    when brake = true then 1.0
                    when brake = false then 0.0
                end
            ),
            2
        ) as brake_sample_pct,

        max(distance_m) as lap_distance_m,

        count(distinct gear) as gears_used,

        count(*) filter (
            where drs in (10, 12, 14)
        ) as drs_active_samples

    from telemetry

    group by
        season,
        round,
        session,
        driver_code,
        driver_key,
        event_key,
        lap_number

)

select * from lap_stats
