{{config(materialized='table')}}

with weather as (
    select * from {{ref('stg_weather')}}
),
events as (
    select event_key, season, round 
    from {{ref('dim_event')}}
), 
final as (
    select 
    {{ dbt_utils.generate_surrogate_key([
            'w.season',
            'w.round',
            'w.time_raw_ns'
        ]) }} as weather_key,

        e.event_key,

        w.season,
        w.round,

        w.time_raw_ns,
        w.session_time_seconds,

        w.air_temp,
        w.track_temp,
        w.track_air_temp_delta,

        w.humidity,
        w.pressure,

        w.rainfall,
        w.weather_condition,

        w.wind_direction,
        w.wind_speed,

        w.is_valid_weather

    from weather w

    left join events e
        on w.season = e.season
        and w.round = e.round

)

select * from final
