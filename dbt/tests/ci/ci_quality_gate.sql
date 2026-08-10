{{ config(severity='error', tags=['ci']) }}

WITH checks AS (
    SELECT
        (SELECT COUNT(*) FROM {{ ref('obt_business') }} WHERE order_id IS NULL) AS obt_null_order_ids,
        (SELECT COUNT(*) FROM {{ ref('obt_business') }} WHERE order_item_id IS NULL) AS obt_null_order_item_ids,
        (SELECT COUNT(*) FROM {{ ref('fact_orders') }} WHERE order_item_id IS NULL) AS fact_null_order_item_ids,
        (SELECT COUNT(*) FROM {{ ref('fact_orders') }} WHERE customer_scd_id IS NULL) AS fact_null_customer_scd_id,
        (SELECT COUNT(*) FROM {{ ref('fact_orders') }} WHERE product_scd_id IS NULL) AS fact_null_product_scd_id,
        (SELECT COUNT(*) FROM {{ ref('fact_orders') }} WHERE store_scd_id IS NULL) AS fact_null_store_scd_id,
        (SELECT COUNT(*) FROM {{ ref('fact_orders') }} WHERE order_scd_id IS NULL) AS fact_null_order_scd_id,
        (SELECT COUNT(*) FROM (
            SELECT order_item_id
            FROM {{ ref('obt_business') }}
            GROUP BY order_item_id
            HAVING COUNT(*) > 1
        ) AS obt_dupes) AS obt_duplicate_order_items,
        (SELECT COUNT(*) FROM (
            SELECT order_item_id
            FROM {{ ref('fact_orders') }}
            GROUP BY order_item_id
            HAVING COUNT(*) > 1
        ) AS fact_dupes) AS fact_duplicate_order_items
)
SELECT *
FROM checks
WHERE obt_null_order_ids > 0
   OR obt_null_order_item_ids > 0
   OR fact_null_order_item_ids > 0
   OR fact_null_customer_scd_id > 0
   OR fact_null_product_scd_id > 0
   OR fact_null_store_scd_id > 0
   OR fact_null_order_scd_id > 0
   OR obt_duplicate_order_items > 0
   OR fact_duplicate_order_items > 0
