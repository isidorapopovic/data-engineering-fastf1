with source as (

    select *
    from read_parquet(
        '../../data/silver/results/**/*.parquet',
        hive_partitioning = true
    )

),

renamed as (

    select

        season,
        round,

        driver_id,
        driver_number,
        driver_code,

        team_id,
        team_name,

        first_name,
        last_name,
        full_name,
        country_code,

        grid_position,
        finish_position,
        classified_position,
        classified_position_numeric,

        time_raw_ns,
        time_seconds as race_time_seconds,
        gap_to_winner_seconds,

        status,
        points,
        laps_completed,

        positions_gained,

        race_winner,
        started_on_pole,
        scored_points

    from source

)

select *
from renamed