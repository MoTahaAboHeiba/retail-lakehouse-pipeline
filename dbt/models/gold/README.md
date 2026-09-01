# Gold Layer Reference

## Architecture

Galaxy schema (fact constellation). Two independent business processes, shared conformed dimensions only where the relationship is real.

![Data model](../../../docs/Data-model.jpg)

## Model Inventory

| Model | Type | Grain |
|---|---|---|
| `fact_orders` | Fact | 1 order line item |
| `fact_supplier_deliveries` | Fact | 1 supplier delivery line |
| `dim_date` | Dimension | 1 calendar date |
| `dim_supplier` | Dimension (SCD1) | 1 supplier |
| `dim_products_current` | Outrigger | 1 current product |
| `eph_*` | Ephemeral | 1 row per business key, sourced from `_tech` tables |

## fact_orders

Point-in-time dimension resolution via `dbt_scd_id` bridge keys, `order_timestamp BETWEEN dbt_valid_from AND dbt_valid_to`, with floor-CTE fallback:

```
BETWEEN dbt_valid_from AND dbt_valid_to
OR (order_timestamp < key's earliest dbt_valid_from AND dbt_valid_from = that earliest value)
```

**Why the fallback:** `dbt_valid_from` is snapshot observation time, not entity creation time. Orders older than a key's first tracked version fall outside every `BETWEEN` window and null-fill silently. Fallback resolves to the earliest known version instead. Computed via precomputed per-key CTE (`MIN(dbt_valid_from) GROUP BY key`), not a correlated subquery

Also carries:
- `product_id` (natural key) — supports `dim_products_current` relationship
- `current_order_status` — inline subquery against current-state `dim_orders` (`dbt_valid_to = 9999-12-31`). Single scalar column, no fan-out risk, no second outrigger needed since `order_status` is the only time-varying attribute on `dim_orders`.

Materialization: full-refresh table. No staleness risk on `current_order_status`.

## fact_supplier_deliveries

Supplier joined on natural key (`dim_supplier` is SCD1). Product resolved to current state (`dbt_valid_to = 9999-12-31`), not point-in-time — a delivery is a transaction, not a product history event.

`delivery_amount = quantity * unit_cost`. Null `unit_cost` preserved via `is_unit_cost_missing` flag, not coalesced.

## dim_date

Dynamic date spine, min/max across both fact sources, 30-day buffer each side. Avoids hardcoded ranges going stale.

## dim_products_current

Current-state filter over `dim_products` (`dbt_valid_to = 9999-12-31`), 1 row per product. Built for Power BI: SCD2 `dim_products` can't support a valid relationship on `product_id` alone (not unique), and `USERELATIONSHIP` requires both candidates independently valid one-to-many, which a non-unique natural key fails structurally.

Carries two keys, not one:
- `dbt_scd_id` → `fact_supplier_deliveries` (already current-state resolved)
- `product_id` → `fact_orders` (resolves point-in-time, won't match current `dbt_scd_id`)

Not a history reintroduction — table is filtered to 1 row/product before either key is read.

**Rejected alternatives:**
- Flatten gold to current-state only — rewrites history on old fact rows
- `USERELATIONSHIP` on full SCD2 `dim_products` — structurally invalid
- Same rejection applied to `dim_orders` role-playing dimension

Grain verified: 500 rows = `COUNT(DISTINCT product_id)` on `dim_products`.

## SCD Strategy

| Dimension | Strategy | Why |
|---|---|---|
| `dim_customers` | SCD2 | Historical accuracy for event analysis |
| `dim_products` | SCD2, dual resolution | Point-in-time for Sales, current for Procurement |
| `dim_stores` | SCD2 | Store context must stay historically consistent |
| `dim_orders` | SCD2 | Order master resolved at event time |
| `dim_supplier` | SCD1 | Reference data, no stated history requirement |

## Testing

## Testing

123/123 tests passing (confirmed via direct `dbt test` execution, 03:45 runtime, PASS=123 WARN=0 ERROR=0 SKIP=0). Every fact carries an independent grain check against source row count. Green tests alone are not accepted as proof, grain and null counts verified directly.

**Open gap:** no direct test coverage on `dim_products` itself (`not_null`/`unique` on `dbt_scd_id`, `product_id`, `dbt_valid_from` not yet added).
