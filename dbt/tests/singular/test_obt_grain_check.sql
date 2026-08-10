{{ config(severity='error') }}

WITH obt_count AS (
    SELECT COUNT(*) AS cnt
    FROM {{ ref('obt_business') }}
),
source_count AS (
    SELECT COUNT(*) AS cnt
    FROM {{ ref('order_items_tech') }}
)
SELECT
    obt_count.cnt AS obt_count,
    source_count.cnt AS source_count
FROM obt_count
CROSS JOIN source_count
WHERE obt_count.cnt != source_count.cnt
