select
    sd.delivery_id               as delivery_id,
    ds.supplier_id               as supplier_id,
    dp.dbt_scd_id                as product_scd_id,
    dd.date_id                   as delivery_date_id,
    sd.quantity                  as quantity,
    sd.unit_cost                 as unit_cost,
    sd.is_unit_cost_missing      as is_unit_cost_missing,
    sd.quantity * sd.unit_cost   as delivery_amount
    
from {{ ref('supplier_deliveries_tech') }} sd
    left join {{ ref('dim_supplier') }} ds
        on sd.supplier_id = ds.supplier_id
    left join {{ ref('dim_products') }} dp
        on sd.product_id = dp.product_id
        and sd.delivery_date between dp.dbt_valid_from and dp.dbt_valid_to
    left join {{ ref('dim_date') }} dd
        on cast(date_format(sd.delivery_date, 'yyyyMMdd') as int) = dd.date_id
