select

    season,
    round,
    driver_id,
    count(*) as row_count

from {{ ref('int_valid_results') }}

group by

    season,
    round,
    driver_id

having count(*) > 1