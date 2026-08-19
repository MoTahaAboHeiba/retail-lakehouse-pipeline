with date_bounds as (

    select
        min(delivery_date) as min_date,
        max(delivery_date) as max_date
    from {{ ref('supplier_deliveries_tech') }}

    union all

    select
        min(order_timestamp) as min_date,
        max(order_timestamp) as max_date
    from {{ ref('orders_tech') }}

),

bounds as (

    select
        date_add(min(min_date), -30) as range_start,
        date_add(max(max_date), 30)  as range_end
    from date_bounds

),

date_spine as (

    select explode(sequence(
        (select range_start from bounds),
        (select range_end from bounds),
        interval 1 day
    )) as full_date

)
select
    cast(date_format(full_date, 'yyyyMMdd') as int) as date_id,
    full_date as calendar_date,
    year(full_date)                                    as year,
    quarter(full_date)                                 as quarter,
    month(full_date)                                   as month,
    date_format(full_date, 'MMMM')                     as month_name,
    day(full_date)                                     as day,
    dayofweek(full_date)                               as day_of_week,
    date_format(full_date, 'EEEE')                     as day_name,
    case when dayofweek(full_date) in (1, 7) then true else false end as is_weekend,
    weekofyear(full_date)                              as week_of_year

from date_spine