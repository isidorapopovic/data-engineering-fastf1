
{{ config(materialized='view') }}

with telemetry as (

    select *
    from {{ ref('stg_telemetry') }}

),

valid_telemetry as (

    select *
    from telemetry

    where season is not null
      and round > 0
      and session is not null
      and driver_code is not null
      and driver_code <> ''
      and lap_number > 0
      and sample_index >= 0

)

select * from valid_telemetry
