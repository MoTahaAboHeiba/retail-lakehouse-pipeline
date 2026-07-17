# Decision Log: employee join removed from the business OBT

## Summary

A defect was identified in the business-layer model `obt_business` where `employees_tech` was joined to `orders_tech` on `store_id`. Because `employees_tech` is not unique at the store level, this caused a cross-product expansion and inflated row counts from the expected grain of order items to roughly 10x the source grain.

## Detection

A manual row-count comparison against the known source grain surfaced the issue:
- `order_items_tech` grain: 30,021 rows
- `obt_business` row count: 300,513 rows

This was not surfaced by `dbt run` or `dbt test`, which passed despite the defect.

## Root cause

The join logic in `obt_business` linked `employees_tech` to `orders_tech` using `store_id`.

This was invalid for two reasons:
1. `store_id` is not unique in `employees_tech`, so the join multiplied rows.
2. The source system never recorded an order-to-employee relationship in `orders_tech`; there is no `employee_id` column on the order grain.

Because the relationship did not exist in the source data, the join should not have been modeled at all.

## Decision

The fix was not to pick an arbitrary employee per store. That would have fabricated a relationship not supported by the source data.

Instead, the decision was:
- remove the employee join from `obt_business`
- remove employee-based fields from downstream fact modeling in `fact_orders`
- preserve `employees_tech` as an independent dimension model feeding its own gold-layer employee model
- keep employee modeling separate from order-level fact grain, consistent with the source-system semantics

## Impact

The downstream model `fact_orders` inherited the same inflated grain because it depended on the corrupted `obt_business` output. Removing the invalid employee join restores the appropriate order-item/line-level grain and prevents the propagation of the defect.

## Business framing

The source system does not capture which employee handled a given order. As a result, employee-level reporting cannot be attached to the order fact grain without inventing a relationship that is not present in the data.

The employee dimension remains valid and useful, but its relationship is to store rather than to individual orders. Employee reporting should be delivered through a separate dimension/analytics pattern instead of being forced into the order fact model.

