{{config(materialized='table')}}

with laps as (
    select * from {{ref('fact_laps')}}
    where tyer_compound is not null and is_valid_lap =true
), 
tyre_stats as(
    select season, round, event_key, driver_key, team_key, tyer_compound, 
    count (*) as valid_laps_on_compound,

        avg(lap_time_raw) as avg_lap_time_raw,

        min(lap_time_raw) as best_lap_time_raw,

        median(lap_time_raw) as median_lap_time_raw,

        stddev_samp(lap_time_raw) as lap_time_stddev_raw,

        avg(tyre_life) as avg_tyre_life,

        min(tyre_life) as min_tyre_life,

        max(tyre_life) as max_tyre_life,

        sum(
            case
                when is_pit_lap then 1
                else 0
            end
        ) as pit_laps

    from laps

    group by
        season,
        round,
        event_key,
        driver_key,
        team_key,
        tyer_compound

),

final as (

    select

        {{ dbt_utils.generate_surrogate_key([
            'season',
            'round',
            'driver_key',
            'tyer_compound'
        ]) }} as tyre_performance_key,

        *

    from tyre_stats

)

select *
from final