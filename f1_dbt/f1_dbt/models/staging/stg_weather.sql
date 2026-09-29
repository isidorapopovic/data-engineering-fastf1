with source as (

    select *
    from read_parquet(
        '../../data/silver/weather/**/*.parquet',
        hive_partitioning = true
    )

),

renamed as (

    select

        season,
        round,

        time_raw_ns,
        session_time_seconds,

        air_temp,
        track_temp,
        track_air_temp_delta,

        humidity,
        pressure,

        rainfall,
        weather_condition,

        wind_direction,
        wind_speed,

        is_valid_weather

    from source

)

select *
from renamed