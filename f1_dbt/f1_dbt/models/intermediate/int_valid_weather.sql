with weather as (

    select *
    from {{ ref('stg_weather') }}

),

valid_weather as (

    select *

    from weather

    where is_valid_weather = true

      and season is not null

      and round > 0

      and session_time_seconds >= 0

      and humidity between 0 and 100

      and wind_speed >= 0

)

select *
from valid_weather