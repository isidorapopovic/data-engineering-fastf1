{{ config(materialized='table') }}

with driver_perf as (

    select *
    from {{ ref('mart_driver_race_performance') }}

),

team_summary as (

    select
        event_key,
        team_key,
        team_name,

        count(distinct driver_key) as drivers,

        sum(points) as team_points,

        avg(grid_position) as avg_grid_position,
        avg(finish_position) as avg_finish_position,

        min(finish_position) as best_finish_position,

        sum(positions_gained) as total_positions_gained,

        sum(valid_laps) as total_valid_laps,

        avg(avg_lap_time_ms) as avg_team_lap_time_ms,

        min(best_lap_time_ms) as best_team_lap_time_ms,

        sum(pit_laps) as total_pit_laps

    from driver_perf

    group by
        event_key,
        team_key,
        team_name
)

select *
from team_summary