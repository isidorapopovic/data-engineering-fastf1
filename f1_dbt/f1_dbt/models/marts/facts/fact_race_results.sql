{{config(materialized='table')}}

with results as (
    select * from {{ref('stg_results')}}
),
drivers as (
    select driver_key, driver_id 
    from {{ref('dim_driver')}}
),
teams as (
    select team_key, team_id
    from {{ref('dim_team')}}
),
events as (
    select event_key, season, round
    from {{ref('dim_event')}}
), 
final as (
    select  {{ dbt_utils.generate_surrogate_key([
            'r.season',
            'r.round',
            'r.driver_id'
        ]) }} as race_result_key,
        e.event_key,
        d.driver_key,
        t.team_key,

        r.season,
        r.round,

        r.grid_position,
        r.finish_position,
        r.classified_position_numeric,
        r.points,
        r.laps_completed,

        r.race_time_seconds,
        r.gap_to_winner_seconds,
        r.positions_gained,

        r.race_winner,
        r.started_on_pole,
        r.scored_points,
        r.status

        from results r
        left join drivers d on r.driver_id=d.driver_id
        left join teams t on r.team_id=t.team_id
        left join events e on r.season=e.season and r.round=e.round
)

select * from final