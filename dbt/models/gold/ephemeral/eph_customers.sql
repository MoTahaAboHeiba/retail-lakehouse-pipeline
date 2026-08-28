SELECT 
    DISTINCT
    customer_id,
    customer_first_name,
    customer_last_name,
    customer_email,
    customer_phone,
    customer_city,
    customer_province,
    customer_country,
    customer_is_active,
    customer_updated_timestamp
FROM 
    {{ ref('obt_business') }}
