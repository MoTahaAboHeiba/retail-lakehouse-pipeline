SELECT 
    DISTINCT
    product_id,
    product_name,
    category,
    brand,
    price,
    product_is_active,
    updated_timestamp as product_updated_timestamp
FROM 
    {{ ref('obt_business') }}
