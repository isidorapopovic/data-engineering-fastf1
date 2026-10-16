{{config(materialized='view')}}

select
cast (season as integer) as season, 
cast (round as integer) as round,
upper(trim(session)) as session,
upper(trim(driver)) as driver,
cast(lap_number as integer) as lap_number,
cast(sample_index as integer) as sample_index,
cast(timestamp_utc as timestamp) as timestamp_utc,
cast(time_seconds as double) as time_seconds,
cast(speed as double) as speed_kmh,
cast(rpm as double) as rpm,
cast(gear as integer) as gear,
cast(throttle as double) as throttle_pct,
cast(brake as boolean) as brake,
cast(drs as integer) as drs,
cast(distance as double) as distance_m

from read_parquet(
    '{{ var("silver_telemetry_path") }}',
    hive_partitioning=false,
    union_by_name=true
)