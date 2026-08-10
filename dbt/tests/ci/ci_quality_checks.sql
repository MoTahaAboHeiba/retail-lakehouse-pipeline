{{ config(severity='error', tags=['ci']) }}

WITH
    obt_count AS (
        SELECT COUNT(*) AS cnt
        FROM {{ ref('obt_business') }}
    ),
    item_count AS (
        SELECT COUNT(*) AS cnt
        FROM {{ ref('order_items_tech') }}
    ),
    fact_count AS (
        SELECT COUNT(*) AS cnt
        FROM {{ ref('fact_orders') }}
    )
SELECT
    obt_count.cnt AS obt_row_count,
    item_count.cnt AS item_row_count,
    fact_count.cnt AS fact_row_count
FROM obt_count
CROSS JOIN item_count
CROSS JOIN fact_count
WHERE obt_count.cnt != item_count.cnt
   OR fact_count.cnt < 1
