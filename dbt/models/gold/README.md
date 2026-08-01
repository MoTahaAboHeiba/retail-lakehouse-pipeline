# Gold Layer Data Models

## Overview

The Gold layer contains the dimensional model of the Walmart Retail Lakehouse.

It is designed for analytical workloads including business intelligence, reporting, ad hoc SQL analysis, and downstream machine learning.

The implementation follows **Kimball dimensional modeling** and is organized as a **Galaxy (Fact Constellation) Schema**, where multiple business processes are modeled independently while sharing common conformed dimensions.

---

# Architecture

# Architecture

```text
                                         GOLD LAYER
                           Galaxy (Fact Constellation) Schema


┌────────────────────────────── SALES ──────────────────────────────┐        ┌──────────────────── PROCUREMENT ─────────────────────┐

        dim_customers (SCD2)                                                    dim_supplier (SCD1)
                │                                                                      │
                │                                                                      │
        dim_stores (SCD2)                                                             │
                │                                                                      │
                │                                                                      │
        dim_orders (SCD2)                                                             │
                │                                                                      │
                │                                                                      │
                ▼                                                                      ▼

        ┌──────────────────────┐                                       ┌────────────────────────────┐
        │     fact_orders      │                                       │ fact_supplier_deliveries   │
        │ Grain: Order Line    │                                       │ Grain: Delivery Line       │
        └──────────┬───────────┘                                       └──────────────┬─────────────┘
                   │                                                                  │
                   │                                                                  │
                   └──────────────┐                                  ┌────────────────┘
                                  │                                  │
                         ┌────────▼──────────────────────────────────▼────────┐
                         │                 Shared Business Dimensions          │
                         │                                                    │
                         │  dim_products (Shared Business Entity)             │
                         │      • SCD2 lookup for Sales                       │
                         │      • Current-state lookup for Procurement        │
                         │                                                    │
                         │  dim_date (Fully Conformed Dimension)              │
                         └────────────────────────────────────────────────────┘
```

---

## Business Processes

The Gold layer models independent business processes rather than source systems.

| Business Process | Fact Table | Grain | Primary Measures | Dimensions |
|-----------------|------------|--------|------------------|------------|
| Sales | `fact_orders` | One order line item | Quantity, Subtotal, Discount, Tax, Total | Customer, Store, Order, Product, Date |
| Procurement | `fact_supplier_deliveries` | One supplier delivery line | Quantity, Unit Cost, Delivery Amount | Supplier, Product, Date |

Each fact is independently modeled according to its business process while sharing common analytical dimensions where appropriate.

---

## Shared Dimensions

Two dimensions are reused across business processes.

| Dimension | Sharing Strategy |
|-----------|------------------|
| **dim_date** | Fully conformed. Both facts use the same calendar dimension and identical join semantics. |
| **dim_products** | Shared business dimension. Both facts analyze the same product entity, but historical resolution differs according to the business process. Sales performs point-in-time SCD2 resolution, whereas Procurement references the current product record because supplier deliveries do not represent changes to product master data. |

This distinction is intentional and preserves the correct historical semantics for each business process without introducing unnecessary dimensional complexity.

---

## Historical Resolution

Historical joins are determined by the analytical requirements of each business process.

| Fact | Product Resolution | Reason |
|------|--------------------|--------|
| `fact_orders` | SCD Type 2 (`dbt_valid_from` / `dbt_valid_to`) | Sales analytics must preserve the historical product attributes that were valid when the order occurred. |
| `fact_supplier_deliveries` | Current product record | Supplier deliveries capture procurement transactions rather than product master data history. Procurement cost changes are represented as facts, not as new product dimension versions. |

This separation ensures that product history remains driven by business attribute changes rather than transactional procurement events.

---

# Why a Galaxy Schema?

A Galaxy (Fact Constellation) Schema models multiple business processes using separate fact tables while allowing them to share common business dimensions.

Current business processes include:

| Business Process | Fact Table |
|-----------------|------------|
| Sales | `fact_orders` |
| Supplier Deliveries | `fact_supplier_deliveries` |

Both facts intentionally reuse the same conformed dimensions:

* Product
* Date

This enables cross-process analytics without duplicating dimensional data.

---

# Conformed Dimensions

The following dimensions are shared across business processes.

| Dimension | Purpose |
|-----------|---------|
| `dim_products` | Shared by Sales and Procurement |
| `dim_date` | Shared calendar dimension |

Conformed dimensions provide a consistent analytical view across the enterprise.

For example:

* Sales vs Procurement trends
* Product profitability
* Purchase cost evolution
* Supplier performance
* Seasonal demand

---

# Dimensions

| Dimension | Type | Grain |
|-----------|------|-------|
| dim_customers | SCD Type 2 | One historical customer version |
| dim_products | SCD Type 2 | One historical product version |
| dim_stores | SCD Type 2 | One historical store version |
| dim_orders | SCD Type 2 | One historical order version |
| dim_supplier | SCD Type 1 | One supplier |
| dim_date | Conformed | One calendar date |

---

# Fact Tables

## fact_orders

### Business Process

Sales

### Grain

One row per order line item.

### Dimensions

* Customer
* Product
* Store
* Order
* Date

### Measures

* Quantity
* Subtotal
* Discount
* Tax
* Total

Historical dimension resolution is performed using SCD Type 2 surrogate keys (`dbt_scd_id`).

---

## fact_supplier_deliveries

### Business Process

Procurement

### Grain

One row per supplier delivery line.

### Dimensions

* Supplier
* Product
* Date

### Measures

* Quantity
* Unit Cost
* Delivery Amount

Business rule:

```
delivery_amount = quantity × unit_cost
```

If `unit_cost` is `NULL`, then `delivery_amount` also remains `NULL`.

Missing values are preserved through the `is_unit_cost_missing` flag.

---

# Historical Tracking Strategy

Only dimensions requiring historical analysis are implemented as Slowly Changing Dimensions.

| Dimension | Strategy |
|-----------|----------|
| Customer | SCD Type 2 |
| Product | SCD Type 2 |
| Store | SCD Type 2 |
| Order | SCD Type 2 |
| Supplier | SCD Type 1 |

Supplier attributes are treated as reference data rather than historical analytical entities.

Consequently, Supplier remains SCD Type 1.

---

# Point-in-Time Dimension Resolution

Historical dimensions are joined using validity windows.

```
dbt_valid_from <= Business Event Date <= dbt_valid_to
```

Examples:

Sales

```
order_timestamp
        │
        ▼
Customer Version
Store Version
Order Version
Product Version
```

Supplier Deliveries

```
delivery_date
        │
        ▼
Product Version
```

This guarantees historical accuracy without modifying transactional facts.

---

# Validation Strategy

A successful `dbt run` or `dbt test` is not considered sufficient evidence of correctness.

Every Gold model is independently validated through SQL using:

* Grain validation
* Row count verification
* Duplicate detection
* Foreign key integrity
* Null handling
* Point-in-time SCD joins
* Measure validation

Only after these validations pass is a model considered production-ready.

---

# Known Design Decisions

## Order Timestamp

The source system does not expose a dedicated business order timestamp.

`created_at` is therefore used as the historical order date proxy.

---

## Supplier History

Supplier deliveries do not represent changes to product master data.

Products are **not** versioned because procurement costs fluctuate.

Instead, procurement history is captured entirely within `fact_supplier_deliveries`.

---

## Margin Analysis

Supplier deliveries cannot be directly matched to customer sales because the source system does not contain batch or lot traceability.

Future margin calculations therefore approximate procurement cost using:

* Product
* Date

This assumption is documented in `DECISION_LOG.md`.

---

# Future Extensions

The current architecture supports adding additional business processes without redesigning existing models.

Potential future facts include:

* Inventory Movements
* Purchase Orders
* Returns
* Inventory Snapshots
* Warehouse Transfers
* Supplier Performance
* Inventory Aging

Because the architecture uses conformed dimensions, new business processes can integrate seamlessly into the existing Galaxy Schema.