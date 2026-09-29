{{config(materialized='table')}}

with teams as (
    select distinct 
    team_id, team_name from {{ref('stg_results')}}
    where team_id is not null
), 

final as (
    select {{dbt_utils.generate_surrogate_key(['team_id'])}} as team_key,
    team_id, team_name
    from teams
)

select * from final