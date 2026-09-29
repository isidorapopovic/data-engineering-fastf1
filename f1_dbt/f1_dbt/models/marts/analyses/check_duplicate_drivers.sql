select
    driver_id,
    count(*) as row_count
from {{ ref('stg_results') }}
where driver_id is not null
group by driver_id
having count(*) > 1
order by row_count desc