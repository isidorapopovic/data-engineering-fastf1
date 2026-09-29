with results as (

    select *
    from {{ ref('stg_results') }}

),

valid_results as (

    select *

    from results

    where season is not null

      and round > 0

      and driver_id is not null

      and driver_code is not null

      and driver_number > 0

      and points >= 0

      and laps_completed >= 0

)

select *
from valid_results