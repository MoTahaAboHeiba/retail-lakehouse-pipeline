with deduped as (

    select
        supplier_id,
        supplier_name,
        delivery_date,
        row_number() over (
            partition by supplier_id
            order by delivery_date desc
        ) as rn

    from {{ ref('supplier_deliveries_tech') }}

)

select
    supplier_id as supplier_key,
    supplier_id,
    supplier_name

from deduped
where rn = 1