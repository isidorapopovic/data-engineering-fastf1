{{ config(
	materialized='table',
	schema='analytics'
) }}

with race_performance as (

	select *
	from {{ ref('mart_driver_race_performance') }}

),

season_summary as (

	select
		season,
		driver_id,
		max(full_name) as driver_name,
		max(team_name) as team_name,

		count(distinct event_key) as races_entered,
		count(distinct case when finish_position = 1 then event_key end) as wins,
		count(distinct case when finish_position <= 3 then event_key end) as podiums,
		count(distinct case when finish_position = 1 then event_key end) as victories,
		count(distinct case when finish_position <= 10 then event_key end) as points_finishes,

		sum(coalesce(points, 0)) as total_points,
		avg(finish_position) as average_finish_position,
		min(finish_position) as best_finish_position,
		count(distinct case when fastest_lap_rank = 1 then event_key end) as fastest_laps,

		count(distinct case
			when lower(coalesce(status, '')) in ('finished', '+1 lap', '+2 laps')
			then event_key
		end) as classified_finishes,
		count(distinct case
			when lower(coalesce(status, '')) not in ('finished', '+1 lap', '+2 laps')
			then event_key
		end) as retirements

	from race_performance
	group by
		season,
		driver_id

)

select
	season,
	driver_id,
	driver_name,
	team_name,
	races_entered,
	wins,
	podiums,
	victories,
	points_finishes,
	total_points,
	average_finish_position,
	best_finish_position,
	fastest_laps,
	classified_finishes,
	retirements,
	round(100.0 * wins / nullif(races_entered, 0), 2) as win_rate_pct,
	round(100.0 * podiums / nullif(races_entered, 0), 2) as podium_rate_pct,
	round(total_points / nullif(races_entered, 0), 2) as points_per_race
from season_summary
