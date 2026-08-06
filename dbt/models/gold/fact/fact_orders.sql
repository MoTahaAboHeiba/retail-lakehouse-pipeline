SELECT
    ob.order_id,
    ob.order_item_id,
    dc.dbt_scd_id AS customer_scd_id,
    dp.dbt_scd_id AS product_scd_id,
    ds.dbt_scd_id AS store_scd_id,
    do_.dbt_scd_id AS order_scd_id,
    dd.date_id AS order_date_id,
    ob.quantity,
    ob.unit_price,
    ob.line_amount,
    ob.total_amount
FROM {{ ref('obt_business') }} ob
LEFT JOIN {{ ref('dim_customers') }} dc
    ON ob.customer_id = dc.customer_id
    AND ob.order_timestamp BETWEEN dc.dbt_valid_from AND dc.dbt_valid_to
LEFT JOIN {{ ref('dim_products') }} dp
    ON ob.product_id = dp.product_id
    AND ob.order_timestamp BETWEEN dp.dbt_valid_from AND dp.dbt_valid_to
LEFT JOIN {{ ref('dim_stores') }} ds
    ON ob.store_id = ds.store_id
    AND ob.order_timestamp BETWEEN ds.dbt_valid_from AND ds.dbt_valid_to
LEFT JOIN {{ ref('dim_orders') }} do_
    ON ob.order_id = do_.order_id
    AND ob.order_timestamp BETWEEN do_.dbt_valid_from AND do_.dbt_valid_to
LEFT JOIN {{ ref('dim_date') }} dd
    ON cast(date_format(cast(ob.order_timestamp as date), 'yyyyMMdd') as int)= dd.date_id
