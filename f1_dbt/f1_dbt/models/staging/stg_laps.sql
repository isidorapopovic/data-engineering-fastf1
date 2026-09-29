select

    season,
    round,

    driver_code,
    driver_number,
    team_name,

    lap_number,
    stint,
    position,

    tyer_compound,
    tyre_life,
    fresh_tyre,

    lap_time_raw,
    sector1_time_raw,
    sector2_time_raw,
    sector3_time_raw,

    speed_i1,
    speed_i2,
    speed_finish_line,
    speed_trap,

    track_status,

    pit_in_time,
    pit_out_time,

    is_pit_lap,
    is_personal_best,
    is_deleted,
    is_accurate,
    is_valid_lap

from read_parquet(
    '{{ var("silver_laps_path") }}',
    hive_partitioning=true
)