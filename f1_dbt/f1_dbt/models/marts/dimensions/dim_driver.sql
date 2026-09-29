{{ config(materialized='table') }}

with source as (

    select
        season,
        round,
        driver_id,
        driver_number,
        driver_code,
        first_name,
        last_name,
        full_name,
        country_code
    from {{ ref('stg_results') }}
    where driver_id is not null

),

ranked as (

    select
        season,
        round,
        driver_id,
        driver_number,
        driver_code,
        first_name,
        last_name,
        full_name,
        country_code,

        row_number() over (
            partition by driver_id
            order by season desc, round desc
        ) as row_num

    from source

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['driver_id']) }} as driver_key,

        driver_id,
        driver_number,
        driver_code,
        first_name,
        last_name,
        full_name,
        country_code

    from ranked

    where row_num = 1

)

select * from final