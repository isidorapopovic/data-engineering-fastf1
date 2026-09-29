select

    season,
    round,
    time_raw_ns,
    count(*) as row_count

from {{ ref('int_valid_weather') }}

group by

    season,
    round,
    time_raw_ns

having count(*) > 1