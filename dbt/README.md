# dbt Transformation Layer

This directory contains the full transformation pipeline for the retail lakehouse project: bronze sources in, gold star schema out. Every layer below is a deliberate architectural decision, not a default. Where a decision traded something off, that tradeoff is stated here, not left for someone else to find.

---

## Pipeline Flow

```text
Bronze (Databricks, via Lakeflow Connect)
        │
        ▼
sources.yml
        │
        ▼
Silver Technical (one model per source table, incremental)
        │
        ▼
Silver Business (metadata-driven One Big Table)
        │
        ▼
Tests (generic + singular, grain verification)
        │
        ▼
Gold Ephemeral (dimension prep, inline CTEs only)
        │
        ▼
Snapshots (SCD Type 2, one file per dimension)
        │
        ▼
Gold Fact (star schema, point-in-time dimension joins)
```

---

## Project Structure

```text
dbt/
├── models/
│   ├── source/
│   │   └── sources.yml
│   ├── silver_tech/
│   │   ├── customers_tech.sql
│   │   ├── employees_tech.sql
│   │   ├── order_items_tech.sql
│   │   ├── orders_tech.sql
│   │   ├── products_tech.sql
│   │   ├── stores_tech.sql
│   │   └── properties.yml
│   ├── silver_business/
│   │   ├── obt_business.sql
│   │   └── schema.yml
│   └── gold/
│       ├── ephemeral/
│       │   ├── eph_customers.sql
│       │   ├── eph_employees.sql
│       │   ├── eph_orders.sql
│       │   ├── eph_products.sql
│       │   └── eph_stores.sql
│       └── fact/
│           ├── fact_orders.sql
│           └── schema.yml
├── snapshots/
│   ├── dim_customers.yml
│   ├── dim_employees.yml
│   ├── dim_orders.yml
│   ├── dim_products.yml
│   └── dim_stores.yml
├── tests/
│   ├── obt_business_grain.sql
│   ├── fact_orders_grain.sql
│   └── singular/
│       ├── test_obt_grain_check.sql
│       └── test_obt_product_join.sql
├── macros/
│   └── custom_schema.sql
├── dbt_project.yml
├── packages.yml
├── profiles.yml
└── DECISION_LOG.md
```

---

# Engineering Decisions

## Sources are declared, never hardcoded

**Decision:** Every bronze table is referenced through `{{ source('bronze', 'table_name') }}`, defined once in `models/source/sources.yml`. No model queries `catalog.schema.table` directly.

**Engineering reasoning:** A hardcoded path compiles and runs fine, but dbt has no record of where that data came from. Lineage graphs, `dbt docs generate`, and impact analysis on a source change all depend on the source declaration existing. Skipping it doesn't cause an error today, it causes an untraceable dependency the day something upstream changes.

---

## Silver technical layer is one model per source table, incremental

**Decision:** Six models (`customers_tech`, `employees_tech`, `orders_tech`, `order_items_tech`, `products_tech`, `stores_tech`), each `materialized='incremental'` with an explicit `unique_key` and merge strategy.

**Engineering reasoning:** This layer does the minimum work: standardize naming, light typing, nothing else. Keeping transformation logic out of this layer means a bug here is isolated to one table's staging pass, not tangled into business logic. Incremental materialization avoids a full rebuild every run, the model only processes what changed since the last run, which matters once table volume stops being trivial.

---

## Silver business layer is metadata-driven, not hardcoded joins

**Decision:** `obt_business.sql` does not contain a hand-written SELECT with hardcoded JOIN clauses. It's built from a structured config (table ref, alias, join key, column list) consumed by a Jinja for-loop that generates the SELECT and JOIN logic at compile time.

**Engineering reasoning:** A hardcoded OBT means adding a 7th source table is a new SQL block written by hand, with every prior join re-verified for correctness by inspection. A metadata-driven OBT means adding a table is one config entry, and the generation logic that produces every other join also produces this one, so there's no new class of bug introduced by a human writing SQL under time pressure. The config uses `ref()`, not hardcoded schema paths, so lineage tracking survives the abstraction instead of being broken by it. See `DECISION_LOG.md`, 2026-07-13, for the full before/after on this.

**Known failure mode this doesn't prevent:** metadata-driven doesn't mean bug-proof. See the fan-out decision below, the fix for that bug was removing a bad join from the config, the config generation itself wasn't what broke.

---

## The employee fan-out bug and why the fix is a removed join, not a smarter join

**What happened:** `employees_tech` was joined into `obt_business` on `store_id`. That column is not unique on `employees_tech`, a store has many employees, so the join produced a cross-product: every order line duplicated once per employee at that store. Row count went from an expected 30,021 (the `order_items_tech` grain) to 300,513. `dbt run` and `dbt test` both passed clean throughout, this was only caught by an independent row-count check against the known source grain.

**Root cause:** `orders_tech` has no `employee_id` column. The source system does not record which employee handled a given order. There is no accurate join between an order and an employee with the data that exists.

**Decision:** Remove the join. Do not approximate it (e.g., arbitrarily picking one employee per store), that fabricates a relationship the source data doesn't support. Employee is retained as its own dimension, connected to the store dimension in gold (snowflake pattern), not to the fact table, since employee's real relationship is to store, not to order.

**Standing practice this created:** every new or changed join now gets a row count check against its expected grain before being called done. A clean `dbt run` and passing tests are not evidence of correctness, they're evidence that nothing errored. Full writeup with stakeholder communication in `DECISION_LOG.md`, 2026-07-13.

---

## Testing: generic tests for structure, singular tests for grain

**Decision:** `not_null`, `unique`, and `relationships` tests cover structural integrity (all four FK pairs tested: order_items to products, order_items to orders, orders to customers, orders to stores). Singular SQL tests (`obt_business_grain.sql`, `fact_orders_grain.sql`) independently verify row count against the known source grain (`order_items_tech`).

**Engineering reasoning:** Generic tests catch broken keys and orphaned records. They do not catch a join that's technically valid SQL but produces the wrong cardinality, that's exactly what the employee fan-out bug was, structurally clean, numerically wrong. Grain tests exist specifically to catch that class of defect, which generic tests cannot see by design.

**Open item, not yet resolved:** `test_obt_grain_check.sql` (under `tests/singular/`) and `obt_business_grain.sql` (top-level) may be redundant, both appear to verify grain on the same model. Not yet reconciled. Flagged here instead of silently keeping duplicate test coverage that looks intentional but isn't.

---

## Ephemeral models exist to prep dimensions, not to be queried

**Decision:** `eph_customers`, `eph_employees`, `eph_orders`, `eph_products`, `eph_stores` are all `materialized='ephemeral'`, compiling as inline CTEs with no standalone table or view in the warehouse.

**Engineering reasoning:** These models select only the columns a given dimension needs before it goes into a snapshot. They don't need to persist independently, they exist purely to keep the snapshot source query clean. Materializing them as tables or views would create warehouse objects with no independent purpose, adding storage and clutter for something that's a compile-time convenience.

---

## Snapshots implement SCD Type 2 declaratively

**Decision:** One snapshot YAML per dimension (`dim_customers`, `dim_employees`, `dim_orders`, `dim_products`, `dim_stores`), timestamp strategy, dbt-managed `dbt_valid_from` / `dbt_valid_to`.

**Engineering reasoning:** Manual SCD2 merge logic is typically hundreds of lines of hand-written merge SQL. dbt reduces this to a YAML config per dimension. One file per dimension instead of one combined file, a broken snapshot is isolated to a single file, not buried in a shared one. Verified correct by running each snapshot twice with a changed source value between runs: exactly one active row per key, `dbt_valid_to` populates correctly on the superseded row, no duplication.

**Default column names (`dbt_valid_from`, `dbt_valid_to`) kept deliberately.** The `dbt_` prefix signals dbt manages and can overwrite these columns. Renaming them without a documented reason weakens the design in an interview, it looks like a customization with no justification behind it.

**Independent SCD2 timelines, not a shared one.** Store and employee dimensions each carry their own `dbt_valid_from`/`dbt_valid_to`, joined to each other by business key (`store_id`) and each filtered for validity independently. Forcing a shared validity window across two dimensions that change on different schedules would produce point-in-time joins that are structurally present but temporally wrong, an employee promotion shouldn't force a store record to appear to change too.

---

## Known limitation inherited from bronze: latest-state-only capture

**Context:** Bronze ingestion (see main project `DECISION_LOG.md`) uses a query-based connector, not true CDC. Only the latest row state per pipeline run reaches bronze, intermediate changes between runs are lost before they ever reach this layer.

**Consequence for this layer:** SCD2 history built in snapshots is only as complete as what bronze delivers. If a dimension attribute changes and reverts between two scheduled ingestion runs, that intermediate state never existed in bronze, so it can never appear in the snapshot history either, this isn't a snapshot logic gap, it's a structural ceiling from two layers upstream. Documented explicitly rather than left for a sharp interviewer to surface first.

---
