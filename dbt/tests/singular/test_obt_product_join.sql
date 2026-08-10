{{ config(severity='error') }}

SELECT *
FROM {{ ref('obt_business') }}
WHERE product_name IS NULL
   OR customer_name IS NULL
   OR store_name IS NULL
