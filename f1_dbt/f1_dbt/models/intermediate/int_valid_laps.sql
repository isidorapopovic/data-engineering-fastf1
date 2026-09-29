with laps as (

    select *
    from {{ref('stg_laps')}}

),

valid_laps as (

    select *
    from laps

    where season is not null
      and round > 0
      and driver_code is not null
      and lap_number > 0
      and is_valid_lap = true

)

select * from valid_laps