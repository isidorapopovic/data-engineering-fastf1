
{{ config(materialized='view') }}

with telemetry as (

    select *
    from {{ ref('int_valid_telemetry') }}

),

drivers as (

    select driver_key, driver_code
    from {{ ref('dim_driver') }}

),

events as (

    select event_key, season, round
    from {{ ref('dim_event') }}

),

final as (

    select

        {{ dbt_utils.generate_surrogate_key([
            't.season',
            't.round',
            't.session',
            't.driver_code',
            't.lap_number',
            't.sample_index'
        ]) }} as telemetry_key,

        e.event_key,
        d.driver_key,

        t.season,
        t.round,
        t.session,
        t.driver_code,
        t.lap_number,
        t.sample_index,

        t.timestamp_utc,
        t.time_seconds,
        t.speed_kmh,
        t.rpm,
        t.gear,
        t.throttle_pct,
        t.brake,
        t.drs,
        t.distance_m

    from telemetry t

    left join drivers d
        on t.driver_code = d.driver_code

    left join events e
        on t.season = e.season
       and t.round = e.round

)

select * from final
