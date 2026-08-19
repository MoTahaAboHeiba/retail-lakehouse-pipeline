SELECT 
    DISTINCT
    product_id,
    product_name,
    category,
    brand,
    price,
    product_is_active
FROM 
    {{ ref('obt_business') }}
