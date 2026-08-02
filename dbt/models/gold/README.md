# Gold Layer Engineering Reference

## Purpose

The Gold layer is the analytics-ready dimensional layer in the Walmart retail lakehouse. It is designed to support governed BI, downstream reporting, and business analysis with stable, conformed semantic structures.

This layer converts raw transaction and operational data into reusable enterprise facts and dimensions while preserving business history through point-in-time joins and explicit dimension versioning.

---

## Architectural Pattern

The Gold layer follows a Galaxy / Fact Constellation model:

- separate fact tables capture distinct business processes
- shared, conformed dimensions are re-used across those processes
- historical resolution is performed at the point of fact generation
- the date dimension is standardized across all analytical consumers

In practice, the current implementation contains two core business facts:

- `fact_orders` for order-line sales analytics
- `fact_supplier_deliveries` for procurement and inbound fulfillment analytics

A conformed date spine and supplier reference dimension support these facts across common reporting workflows.

---

## Gold Layer Model Inventory

| Model | Type | Grain | Purpose |
|---|---|---|---|
| `fact_orders` | Fact | One order line item | Sales analytics using point-in-time customer, store, order, product, and date context |
| `fact_supplier_deliveries` | Fact | One supplier delivery line | Procurement analytics based on supplier, product, and delivery date |
| `dim_date` | Dimension | One calendar date | Shared calendar spine for both transactions and delivery reporting |
| `dim_supplier` | Dimension | One supplier record | Supplier reference dimension with latest-state deduplication |
| `eph_*` models | Ephemeral staging | Intermediate business slices | Pre-fact shaping for dimension and analytics preparation |

---

## Data Lineage and Source Semantics

### 1. Sales fact lineage

`fact_orders` is built from `obt_business` and joins to historical snapshot dimensions using the event time `order_timestamp`.

The fact stores the following business keys and bridge keys:

- `customer_scd_id`
- `product_scd_id`
- `store_scd_id`
- `order_scd_id`
- `order_date_id`

The join predicates use validity windows of the form:

```sql
order_timestamp BETWEEN dim.dbt_valid_from AND dim.dbt_valid_to
```

This ensures that each order line is resolved against the dimension version that was valid when the business event happened.

### 2. Procurement fact lineage

`fact_supplier_deliveries` is built from `supplier_deliveries_tech` and joins supplier reference data plus point-in-time product history.

The fact includes:

- `supplier_id`
- `product_scd_id`
- `delivery_date_id`
- `quantity`
- `unit_cost`
- `is_unit_cost_missing`
- `delivery_amount`

The calculated measure is:

```sql
delivery_amount = quantity * unit_cost
```

When `unit_cost` is null, the derived amount remains null and the source missingness is preserved through the `is_unit_cost_missing` flag.

### 3. Calendar dimension lineage

`dim_date` is generated from a dynamic date spine using the minimum and maximum dates in the operational source tables, with a 30-day forward and backward buffer.

This is intentionally resilient to data arrival timing and avoids hard-coding a date range that would drift out of sync with new business activity.

---

## Gold Layer Design Decisions

### Fact constellation

The current Gold layer is intentionally split by business process rather than by source system.

This means:

- `fact_orders` encapsulates order-level commercial behavior
- `fact_supplier_deliveries` encapsulates procurement and receipt behavior
- both facts can be analyzed together through shared conformed dimensions such as `dim_date` and the product history model

### Historical resolution strategy

The implementation uses a mix of SCD patterns:

| Dimension | Strategy | Rationale |
|---|---|---|
| `dim_customers` | SCD Type 2 | Customer attributes must remain historically accurate for retail event analysis |
| `dim_products` | SCD Type 2 | Product metadata changes must be captured at the point of business activity |
| `dim_stores` | SCD Type 2 | Store context may evolve over time and must remain historically consistent |
| `dim_orders` | SCD Type 2 | Order master context should retain the version that was valid when the event occurred |
| `dim_supplier` | SCD Type 1 | Supplier reference data is treated as current-state dimensional lookup data |

### Conformed dimension usage

The date dimension is fully conformed across the fact constellation. Product history is also shared by the fact set, but it is resolved at the event timestamp appropriate to the business process.

This is the key design pattern that keeps the warehouse analytically consistent while preserving event-level correctness.

---

## Table-by-Table Engineering Notes

### `fact_orders`

- Model materialization: analytical table
- Grain: one row per order line item
- Core measures: `quantity`, `unit_price`, `line_amount`, `total_amount`
- Key design: point-in-time dimension resolution via SCD surrogate bridge keys (`dbt_scd_id`)
- Primary reporting value: purchase and sales transaction analysis at order item granularity

### `fact_supplier_deliveries`

- Model materialization: analytical table
- Grain: one row per supplier delivery line
- Core measures: `quantity`, `unit_cost`, `delivery_amount`
- Key design: supplier reference join plus product SCD resolution for the delivery date
- Primary reporting value: procurement cost and receipts analysis at delivery-line granularity

### `dim_date`

- Model materialization: analytical table
- Grain: one row per calendar day
- Purpose: shared temporal anchor for all gold analytics
- Generated fields include:
  - `date_id` as `yyyyMMdd`
  - `calendar_date`
  - `year`, `quarter`, `month`, `month_name`
  - `day`, `day_of_week`, `day_name`
  - `is_weekend`
  - `week_of_year`

### `dim_supplier`

- Model materialization: analytical table
- Grain: one current supplier record
- Source behavior: latest record per supplier is retained after deduplication on `delivery_date` descending
- Purpose: stable supplier reference data for procurement fact joins

---

## dbt Testing and Data Quality Intent

The Gold layer is governed through schema-level data tests and analytical validation patterns. The implementation is intended to enforce:

- non-null and uniqueness constraints on business keys
- referential integrity between facts and dimensions
- preservation of missing-cost semantics in delivery measures
- point-in-time correctness across historical dimension joins

These contracts keep the layer usable for downstream reporting and prevent silent degradation of business semantics.

---

## Operational Notes

### Build pattern

The Gold layer sits downstream of the Silver/Business semantic layer and upstream of downstream consumption. Its contracts should therefore favor:

- stable grains
- conformed keys
- explicit historical version resolution
- minimal logic in downstream consumers

### Why the model stays compact

The current implementation intentionally limits the gold layer to the most valuable analytic entities for the current warehouse scope:

- one sales fact
- one procurement fact
- two supporting dimensions with shared conformance behavior

This keeps lineage understandable and makes the layer easier to test, document, and extend.

---

## Summary

This Gold layer is a production-oriented analytics layer that translates operational event data into stable, historical, and query-friendly fact and dimension models. The implementation is intentionally grounded in practical warehouse engineering concerns: business grain, conformance, historical correctness, testability, and clear lineage.

It is not a conceptual-only design document; it reflects the actual dbt artifact set, the implemented join semantics, and the observable business expectations encoded in the current dbt project.