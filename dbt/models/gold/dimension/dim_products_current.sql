SELECT
    dbt_scd_id,
    product_id,
    product_name,
    category,
    brand,
    price,
    product_is_active
FROM {{ ref('dim_products') }}
WHERE dbt_valid_to = to_date('9999-12-31')
