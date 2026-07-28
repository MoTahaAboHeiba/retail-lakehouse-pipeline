{{
    config(
        materialized='incremental',
        unique_key='delivery_id',
        incremental_strategy='merge'
    )
}}

with source as (

    select *
    from {{ source('walmart_databricks', 'supplier_deliveries_b') }}

    {% if is_incremental() %}
    where delivery_date > (select coalesce(max(delivery_date), '1900-01-01') from {{ this }})
    {% endif %}

),

deduped as (

    select
        *,
        row_number() over (
            partition by delivery_id
            order by delivery_date desc
        ) as rn

    from source

),

cleaned as (

    select
        cast(delivery_id as string)      as delivery_id,
        cast(product_id as string)       as product_id,
        cast(supplier_id as string)      as supplier_id,
        cast(delivery_date as date)      as delivery_date,
        cast(quantity_received as int)   as quantity,
        cast(unit_cost as decimal(10,2)) as unit_cost,
        unit_cost is null                as is_unit_cost_missing,
        current_timestamp()              as processed_at

    from deduped
    where rn = 1

)

select * from cleaned