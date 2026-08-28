SELECT
    order_id,
    payment_method,
    order_status,
    order_timestamp,
    is_active as order_is_active,
    updated_timestamp as order_updated_timestamp
FROM {{ ref('orders_tech') }}
