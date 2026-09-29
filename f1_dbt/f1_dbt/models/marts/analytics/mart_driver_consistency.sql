{{config(materialized='table')}}

with laps as (
    select * from {{ref('fact_laps')}}
    where is_valid_lap = true
),

stats as (

    select
        event_key,
        driver_key,

        count(*) as valid_laps,

        avg(lap_time_raw) as avg_lap_time,
        median(lap_time_raw) as median_lap_time,
        stddev_samp(lap_time_raw) as lap_time_stddev,

        min(lap_time_raw) as best_lap_time,
        max(lap_time_raw) as worst_lap_time,

        max(lap_time_raw) - min(lap_time_raw) as pace_range,

        stddev_samp(lap_time_raw)
            / nullif(avg(lap_time_raw), 0)
            as coefficient_of_variation

    from laps

    group by
        event_key,
        driver_key
)

select *
from stats