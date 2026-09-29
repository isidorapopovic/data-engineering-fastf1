{{config(materialized='table')}}

with laps as (
    select * from {{ref('int_valid_laps')}}
), 
driver as (
    select driver_key, driver_id, driver_code 
    from {{ref('dim_driver')}}
),
teams as (
    select team_key, team_id, team_name
    from {{ref('dim_team')}}
),

events as (
    select event_key, season, round
    from {{ref('dim_event')}}
),

final as (
    select 
    {{ dbt_utils.generate_surrogate_key([
        'l.season','l.round','l.driver_code','l.lap_number'
    ])}} as lap_key,
    e.event_key, d.driver_key, t.team_key,
        l.season,
        l.round,
        l.lap_number,
        l.stint,
        l.position,

        l.tyer_compound,
        l.tyre_life,
        
        l.lap_time_raw,
        l.sector1_time_raw,
        l.sector2_time_raw,
        l.sector3_time_raw,

        l.speed_i1,
        l.speed_i2,
        l.speed_finish_line,
        l.speed_trap,

        l.track_status,
        l.pit_in_time,
        l.pit_out_time,

        l.is_pit_lap,
        l.is_personal_best,
        l.is_deleted,
        l.is_accurate,
        l.is_valid_lap

        from laps l 
        left join driver d on l.driver_code=d.driver_code
        left join teams t on l.team_name=t.team_name
        left join events e on l.season=e.season and l.round=e.round
)

select * from final