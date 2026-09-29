{{ config(
    materialized='table'
) }}

select distinct

    {{ dbt_utils.generate_surrogate_key([
        'season',
        'round'
    ]) }} as event_key,

    season,
    round

from {{ ref('stg_results') }}