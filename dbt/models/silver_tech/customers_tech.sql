{{
    config(
        materialized='incremental',
        unique_key='customer_id',
        incremental_strategy='merge'
    )
}}

with source as (

    select
        customer_id,
        first_name,
        last_name,
        email,
        phone,
        city,
        province,
        country,
        created_timestamp,
        updated_timestamp,
        is_active
    from {{ source('walmart_databricks', 'customers') }}

    {% if is_incremental() %}
    where updated_timestamp > (
        select coalesce(max(updated_timestamp), '1900-01-01')
        from {{ this }}
    )
    {% endif %}

),

deduped as (

    select
        customer_id,
        first_name,
        last_name,
        email,
        phone,
        city,
        province,
        country,
        created_timestamp,
        updated_timestamp,
        is_active,
        row_number() over (
            partition by customer_id
            order by updated_timestamp desc
        ) as rn

    from source

),

cleaned as (

    select
        customer_id,
        first_name,
        last_name,
        email,
        phone,
        city,
        province,
        country,
        created_timestamp,
        updated_timestamp,
        is_active,
        current_timestamp() as processed_at

    from deduped
    where rn = 1

)

select
    customer_id,
    first_name,
    last_name,
    email,
    phone,
    city,
    province,
    country,
    created_timestamp,
    updated_timestamp,
    is_active,
    processed_at
from cleaned