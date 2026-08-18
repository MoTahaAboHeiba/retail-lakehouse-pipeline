SELECT
    order_id,
    payment_method,
    order_status,
    order_timestamp,
    created_timestamp as order_created_timestamp,
    updated_timestamp as order_updated_timestamp,
    is_active as order_is_active
FROM {{ ref('orders_tech') }}