{{
    config(
        materialized='incremental',
        unique_key='employee_id'
    )
}}

with source as (

    select *
    from {{ source('walmart_databricks', 'employees') }}

    {% if is_incremental() %}
    where updated_timestamp > (
        select coalesce(max(updated_timestamp), '1900-01-01')
        from {{ this }}
    )
    {% endif %}

),

deduped as (

    select
        *,
        row_number() over (
            partition by employee_id
            order by updated_timestamp desc
        ) as rn

    from source

),

cleaned as (

    select
        * except (rn),
        current_timestamp() as processed_at

    from deduped
    where rn = 1

)

select * from cleaned