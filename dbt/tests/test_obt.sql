{{ config(severity='error') }}

SELECT 1 
FROM 
    {{ ref('obt_business') }} AS obt
WHERE 
    obt.order_id IS NULL
OR
    obt.product_id IS NULL
OR
    obt.store_id IS NULL
OR
    obt.order_item_id IS NULL
OR
    obt.customer_id IS NULL
