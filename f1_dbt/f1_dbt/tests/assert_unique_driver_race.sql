select season, round, driver_key, count(*) as row_count

from {{ref('assert_unique_driver_race.sql')}}

group by season, round, driver_key

having count(*)>1