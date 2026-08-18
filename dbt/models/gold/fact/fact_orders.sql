-- Works correctly for an ongoing/live pipeline where SCD2 tracking
-- starts at or before the first fact event for every key
-- Not valid here because this data was backfilled before tracking began 
-- see the gold's README.md for more details

--  SELECT
--     ob.order_id,
--     ob.order_item_id,
--     dc.dbt_scd_id AS customer_scd_id,
--     dp.dbt_scd_id AS product_scd_id,
--     ds.dbt_scd_id AS store_scd_id,
--     do_.dbt_scd_id AS order_scd_id,
--     dd.date_id AS order_date_id,
--     ob.quantity,
--     ob.unit_price,
--     ob.line_amount,
--     ob.total_amount
-- FROM {{ ref('obt_business') }} ob
-- LEFT JOIN {{ ref('dim_customers') }} dc
--     ON ob.customer_id = dc.customer_id
--     AND ob.order_timestamp BETWEEN dc.dbt_valid_from AND dc.dbt_valid_to
-- LEFT JOIN {{ ref('dim_products') }} dp
--     ON ob.product_id = dp.product_id
--     AND ob.order_timestamp BETWEEN dp.dbt_valid_from AND dp.dbt_valid_to
-- LEFT JOIN {{ ref('dim_stores') }} ds
--     ON ob.store_id = ds.store_id
--     AND ob.order_timestamp BETWEEN ds.dbt_valid_from AND ds.dbt_valid_to
-- LEFT JOIN {{ ref('dim_orders') }} do_
--     ON ob.order_id = do_.order_id
--     AND ob.order_timestamp BETWEEN do_.dbt_valid_from AND do_.dbt_valid_to
-- LEFT JOIN {{ ref('dim_date') }} dd
--     ON cast(date_format(cast(ob.order_timestamp as date), 'yyyyMMdd') as int) = dd.date_id


-- Floor CTE version, passes tests (backfilled data, tracking starts late)
WITH customer_floor AS (
    SELECT customer_id, MIN(dbt_valid_from) AS earliest_valid_from
    FROM {{ ref('dim_customers') }}
    GROUP BY customer_id
),
product_floor AS (
    SELECT product_id, MIN(dbt_valid_from) AS earliest_valid_from
    FROM {{ ref('dim_products') }}
    GROUP BY product_id
),
store_floor AS (
    SELECT store_id, MIN(dbt_valid_from) AS earliest_valid_from
    FROM {{ ref('dim_stores') }}
    GROUP BY store_id
),
order_floor AS (
    SELECT order_id, MIN(dbt_valid_from) AS earliest_valid_from
    FROM {{ ref('dim_orders') }}
    GROUP BY order_id
)
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
LEFT JOIN customer_floor cf ON ob.customer_id = cf.customer_id
LEFT JOIN {{ ref('dim_customers') }} dc
    ON ob.customer_id = dc.customer_id
    AND (
        ob.order_timestamp BETWEEN dc.dbt_valid_from AND dc.dbt_valid_to
        OR (ob.order_timestamp < cf.earliest_valid_from AND dc.dbt_valid_from = cf.earliest_valid_from)
    )
LEFT JOIN product_floor pf ON ob.product_id = pf.product_id
LEFT JOIN {{ ref('dim_products') }} dp
    ON ob.product_id = dp.product_id
    AND (
        ob.order_timestamp BETWEEN dp.dbt_valid_from AND dp.dbt_valid_to
        OR (ob.order_timestamp < pf.earliest_valid_from AND dp.dbt_valid_from = pf.earliest_valid_from)
    )
LEFT JOIN store_floor sf ON ob.store_id = sf.store_id
LEFT JOIN {{ ref('dim_stores') }} ds
    ON ob.store_id = ds.store_id
    AND (
        ob.order_timestamp BETWEEN ds.dbt_valid_from AND ds.dbt_valid_to
        OR (ob.order_timestamp < sf.earliest_valid_from AND ds.dbt_valid_from = sf.earliest_valid_from)
    )
LEFT JOIN order_floor ordf ON ob.order_id = ordf.order_id
LEFT JOIN {{ ref('dim_orders') }} do_
    ON ob.order_id = do_.order_id
    AND (
        ob.order_timestamp BETWEEN do_.dbt_valid_from AND do_.dbt_valid_to
        OR (ob.order_timestamp < ordf.earliest_valid_from AND do_.dbt_valid_from = ordf.earliest_valid_from)
    )
LEFT JOIN {{ ref('dim_date') }} dd
    ON cast(date_format(cast(ob.order_timestamp as date), 'yyyyMMdd') as int) = dd.date_id
