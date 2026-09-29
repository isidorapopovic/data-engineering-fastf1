select 
season, 
round, 
driver_code,
lap_number,
count(*) as row_count
from {{ref ('stg_laps')}}
group by season, round, driver_code, lap_number
having count(*)>1