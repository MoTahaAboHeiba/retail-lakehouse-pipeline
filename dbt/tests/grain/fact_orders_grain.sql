{{ config(severity='error', tags=['ci']) }}

SELECT
    fo.fo_count
FROM (
    SELECT COUNT(*) AS fo_count
    FROM {{ ref('fact_orders') }}
) AS fo
CROSS JOIN (
    SELECT COUNT(*) AS item_count
    FROM {{ ref('order_items_tech') }}
) AS oi
WHERE fo.fo_count <> oi.item_count