{{ config(severity='error') }}

SELECT
    obt.obt_count
FROM (
    SELECT COUNT(*) AS obt_count
    FROM {{ ref('obt_business') }}
) AS obt
CROSS JOIN (
    SELECT COUNT(*) AS item_count
    FROM {{ ref('order_items_tech') }}
) AS oi
WHERE obt.obt_count <> oi.item_count
