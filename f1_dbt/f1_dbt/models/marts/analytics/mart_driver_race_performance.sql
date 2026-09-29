{{config(materialized='table')}}

with lap_stats as (
    select event_key, driver_key, count(*) as total_laps,
    sum(case when is_valid_lap then 1 else 0
    end
    ) as valid_laps,
    avg(case when is_valid_lap then lap_time_raw
    end) as avg_lap_time_ms,
    min (case when is_valid_lap then lap_time_raw end) as best_lap_time_ms,
    percentile_cont(0.5) within group (
    order by case when is_valid_lap then lap_time_raw end ) as median_lap_time_ms,
    avg(
        case when is_valid_lap then speed_trap end
    ) as avg_speed_trap,
    max(speed_trap) as max_speed_trap,
    sum (case when is_pit_lap then 1 else 0 end
    ) as pit_laps,

    count(distinct stint) as stint_count

    from {{ref('fact_laps')}}
    group by event_key, driver_key

),

lap_ranked as (

    select
        *,
        rank() over (
            partition by event_key
            order by best_lap_time_ms
        ) as fastest_lap_rank

    from lap_stats

),


race_results as (

    select
        race_result_key,
        event_key,
        driver_key,
        team_key,

        grid_position,
        finish_position,
        classified_position_numeric,

        points,
        laps_completed,
        positions_gained,

        race_time_seconds,
        gap_to_winner_seconds,

        race_winner,
        started_on_pole,
        scored_points,

        status

    from {{ ref('fact_race_results') }}
),

drivers as (

    select
        driver_key,
        driver_id,
        driver_code,
        full_name,
        country_code

    from {{ ref('dim_driver') }}
),

teams as (

    select
        team_key,
        team_id,
        team_name

    from {{ ref('dim_team') }}
),

events as (

    select
        event_key,
        season,
        round

    from {{ ref('dim_event') }}
),

final as (

    select
        r.race_result_key,

        e.event_key,
        d.driver_key,
        t.team_key,

        e.season,
        e.round,

        d.driver_id,
        d.driver_code,
        d.full_name,
        d.country_code,

        t.team_id,
        t.team_name,

        r.grid_position,
        r.finish_position,
        r.classified_position_numeric,

        r.points,
        r.laps_completed,
        r.positions_gained,

        r.race_time_seconds,
        r.gap_to_winner_seconds,

        r.race_winner,
        r.started_on_pole,
        r.scored_points,

        r.status,

        l.total_laps,
        l.valid_laps,

        l.avg_lap_time_ms,
        l.best_lap_time_ms,
        l.median_lap_time_ms,
        l.fastest_lap_rank,

        l.avg_speed_trap,
        l.max_speed_trap,

        l.pit_laps,
        l.stint_count

    from race_results r

    left join lap_ranked l
    on r.event_key = l.event_key
    and r.driver_key = l.driver_key

    left join drivers d
        on r.driver_key = d.driver_key

    left join teams t
        on r.team_key = t.team_key

    left join events e
        on r.event_key = e.event_key
)

select *
from final