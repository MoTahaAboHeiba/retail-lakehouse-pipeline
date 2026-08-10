{{
    config(
        materialized='incremental',
        unique_key='order_id'
    )
}}

with source as (

    select
        order_id,
        customer_id,
        store_id,
        order_timestamp,
        payment_method,
        order_status,
        total_amount,
        created_timestamp,
        updated_timestamp,
        is_active
    from {{ source('walmart_databricks', 'orders') }}

    {% if is_incremental() %}
    where updated_timestamp > (
        select coalesce(max(updated_timestamp), '1900-01-01')
        from {{ this }}
    )
    {% endif %}

),

deduped as (

    select
        order_id,
        customer_id,
        store_id,
        order_timestamp,
        payment_method,
        order_status,
        total_amount,
        created_timestamp,
        updated_timestamp,
        is_active,
        row_number() over (
            partition by order_id
            order by updated_timestamp desc
        ) as rn

    from source

),

cleaned as (

    select
        order_id,
        customer_id,
        store_id,
        order_timestamp,
        payment_method,
        order_status,
        total_amount,
        created_timestamp,
        updated_timestamp,
        is_active,
        current_timestamp() as processed_at

    from deduped
    where rn = 1

)

select
    order_id,
    customer_id,
    store_id,
    order_timestamp,
    payment_method,
    order_status,
    total_amount,
    created_timestamp,
    updated_timestamp,
    is_active,
    processed_at
from cleaned