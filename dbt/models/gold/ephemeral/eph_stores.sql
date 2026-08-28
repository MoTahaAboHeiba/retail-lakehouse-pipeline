SELECT 
    DISTINCT
    store_id,
    store_name,
    store_city,
    store_province,
    store_country,
    store_is_active,
    updated_timestamp as store_updated_timestamp
FROM 
    {{ ref('obt_business') }}
