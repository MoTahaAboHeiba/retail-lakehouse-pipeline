{{
    config(
        materialized='incremental',
        unique_key='order_item_id'
    )
}}

with source as (

    select
        order_item_id,
        order_id,
        product_id,
        quantity,
        unit_price,
        line_amount,
        created_timestamp,
        updated_timestamp,
        is_active
    from {{ source('walmart_databricks', 'order_items') }}

    {% if is_incremental() %}
    where updated_timestamp > (
        select coalesce(max(updated_timestamp), '1900-01-01')
        from {{ this }}
    )
    {% endif %}

),

deduped as (

    select
        order_item_id,
        order_id,
        product_id,
        quantity,
        unit_price,
        line_amount,
        created_timestamp,
        updated_timestamp,
        is_active,
        row_number() over (
            partition by order_item_id
            order by updated_timestamp desc
        ) as rn

    from source

),

cleaned as (

    select
        order_item_id,
        order_id,
        product_id,
        quantity,
        unit_price,
        line_amount,
        created_timestamp,
        updated_timestamp,
        is_active,
        current_timestamp() as processed_at

    from deduped
    where rn = 1

)

select
    order_item_id,
    order_id,
    product_id,
    quantity,
    unit_price,
    line_amount,
    created_timestamp,
    updated_timestamp,
    is_active,
    processed_at
from cleaned