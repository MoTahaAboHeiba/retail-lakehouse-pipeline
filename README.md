# Retail Lakehouse Pipeline

End-to-end data engineering pipeline for retail data. Postgres OLTP source, Databricks lakehouse (bronze/silver/gold), dbt for transformation and testing, Airflow for orchestration, S3 as a secondary ingestion path, CI gating on dbt tests planned as a final step.

**Status: core pipeline built end to end, gold layer under active data validation.** Bronze through silver is complete and verified. Gold layer objects (dimensions, both fact tables) are built and orchestrated, but the most recent full test run surfaced integrity defects in both fact tables that are being root-caused before this layer is called closed. This README states that plainly rather than hiding it behind a passing test count. CI is scoped as the next phase, not yet built.

For the full engineering reasoning behind each layer, see:

- [`dataset/README.md`](https://github.com/MoTahaAboHeiba/retail-lakehouse-pipeline/blob/main/dataset/README.md), PostgreSQL database on ghost.build, why, and how to use it
- [`dbt/README.md`](https://github.com/MoTahaAboHeiba/retail-lakehouse-pipeline/blob/main/dbt/README.md), transformation layer decisions, the fan-out bug, testing strategy, SCD2 design, gold layer defect log
- [`airflow/README.md`](https://github.com/MoTahaAboHeiba/retail-lakehouse-pipeline/blob/main/airflow/README.md), orchestration decisions, Docker issues found and fixed, credential handling

This file stays high-level. The subsystem READMEs carry the depth.

---

## Why this project exists

I built this to go deep on two areas I identified as gaps in my own skill set: dbt (incremental models, snapshots, metadata-driven transformation) and Docker (containerized orchestration). Databricks/Spark and Airflow fundamentals I already had going in, so time here is weighted toward the parts that are actually new to me.

This is not a from-scratch architecture idea. It is based on a published retail data engineering tutorial (Postgres to Databricks to dbt to Airflow). I am not pretending otherwise. What I control is the engineering decisions on top of that structure, and this README documents those decisions honestly, including the ones that involved a tradeoff, a limitation I chose to accept, or a bug I found and fixed.

## Architecture

```
Postgres (OLTP source)
    -> Databricks bronze (query-based incremental ingestion via Lakeflow Connect)
    -> AWS S3 (secondary ingestion path, notebook-driven Auto Loader) -> Databricks bronze
    -> dbt silver technical layer (per-table incremental models)
    -> dbt silver business layer (metadata-driven One Big Table)
    -> dbt tests (generic + singular)
    -> dbt snapshots (SCD Type 2 dimension history)
    -> dbt gold layer (galaxy / fact constellation schema: two business processes, shared conformed dimensions)
    -> Airflow (Docker) orchestrates the chain end to end
    -> GitHub Actions CI gates every push on dbt test results (planned, not yet built)
```

The gold layer is a galaxy schema (fact constellation), not a single star schema. Two business processes, Sales and Procurement, share conformed dimensions (`dim_product`, `dim_date`) while each maintaining its own fact table at its own grain. Full reasoning in `dbt/README.md`.

## Tech stack and why each piece is there

| Tool | Role | Why |
|---|---|---|
| Databricks (Lakeflow Connect, serverless) | Bronze ingestion, primary source | Query-based incremental load using cursor column + primary key. This is deliberately not labeled CDC, see below. |
| Databricks Auto Loader (notebook-driven) | Bronze ingestion, secondary source | S3-based supplier delivery feed. Chosen after hitting `QUOTA_EXCEEDED_EXCEPTION` on the Databricks Free Edition ingestion UI (single active pipeline limit). Auto Loader gives incremental, production-grade ingestion without depending on that UI. |
| dbt Core + dbt-databricks | Silver/gold transformation | Incremental models, SCD2 snapshots, metadata-driven OBT via Jinja, galaxy schema gold layer. |
| Airflow (Docker) | Orchestration | Triggers dbt runs and Databricks jobs. No transformation logic lives inside a DAG task. |
| AWS S3 | Secondary ingestion path | Represents a second source system feeding the same lakehouse, distinct from the live OLTP path. Built and scheduled. |
| GitHub Actions | CI | Runs dbt tests on every push, blocks merge on failure. Scoped as next phase, not yet built. |

## Important: ingestion pattern is not CDC

Databricks Free Edition is serverless-only. True CDC through Lakeflow Connect's PostgreSQL connector requires a continuous classic-compute gateway to consume the write-ahead log through logical replication, and Free Edition cannot provision that gateway.

I use Lakeflow Connect's query-based connector instead: a cursor column plus a primary key per table, driving scheduled incremental upserts. This runs fully serverless with no gateway requirement.

The tradeoff I accepted: this is scheduled polling, not continuous capture, and each run captures only the latest row state, not every intermediate change between runs. I account for this explicitly in the snapshot design rather than discovering it as a surprise at the dimension history stage.

## Engineering decision: metadata-driven silver business layer

The silver business layer (`obt_business`) is not a hardcoded SELECT with hardcoded joins. It is built from a structured config (table reference, join key, column list per source) that a Jinja for-loop compiles into the SELECT and JOIN clauses at build time. Adding a new source table to this model means adding one config entry, not writing new SQL.

The config references source models through dbt's `ref()`, not hardcoded schema paths, so lineage tracking through `dbt docs generate` survives the abstraction instead of being broken by it. Full reasoning in `dbt/README.md`.

## Engineering decision: found and fixed a 10x row inflation bug via independent verification

During silver business layer build, `obt_business` produced 300,513 rows against an expected grain of 30,021 (the `order_items` row count). Every `dbt run` and `dbt test` had passed clean, none of it caught this.

Root cause: the model joined `employees` into the OBT on `store_id`, which is not a unique key on that table. A store has many employees, so the join produced a cross-product, every order line duplicated once per employee at that store. Deeper cause: the orders table has no `employee_id` column at all, the source system does not record which employee handled a given order, so no accurate order-to-employee join is possible with this data.

Fix: removed the join rather than approximating a relationship the data doesn't support. Employee is retained as its own dimension, connected to the store dimension in gold (snowflake pattern) instead of to the fact table. Full root cause, stakeholder communication, and star schema placement reasoning in `dbt/README.md` and `DECISION_LOG.md`.

Standing practice this created: every new or changed join gets a row count check against its expected grain before being considered done. A green `dbt run` confirms the model executed. It does not confirm the numbers are right. This practice is what surfaced the gold layer defects listed below, not a coincidence.

## Engineering decision: `dim_product` is a shared but not fully conformed dimension

`dim_product` feeds both business processes, but each resolves it differently, on purpose:

- **Sales (`fact_orders`)** resolves `dim_product` at point-in-time, matching each order to the product version that was active on that order's date via SCD2 `dbt_valid_from`/`dbt_valid_to`.
- **Procurement (`fact_supplier_deliveries`)** resolves to the current product record only, not point-in-time.

Reasoning: a supplier delivery is a procurement transaction, not a product history event. Procurement cost fluctuations belong on the delivery fact as measures, not as a driver of new product dimension versions. Treating procurement cost changes as SCD2-worthy product history would conflate two different business questions, what a product is, versus what it cost to acquire, into one timeline.

## Engineering decision: `dim_supplier` as SCD1 with no surrogate key

Supplier attributes (`supplier_id`, `supplier_name`) are treated as low-volatility reference data, not slowly changing history worth tracking. No stated business requirement exists to know what a supplier was previously named. `fact_supplier_deliveries` joins directly on the natural key `supplier_id`, no surrogate key layer added. This is a deliberate scope decision: adding SCD2 machinery to a two-column reference dimension is complexity with no requirement behind it, not rigor.

## Engineering decision: `dim_date` as a dynamically generated conformed dimension

`dim_date` is shared across both fact tables (`fact_orders` via `order_date_id`, `fact_supplier_deliveries` via `delivery_date_id`). Its date range is not hardcoded. It is derived at build time from the actual min/max dates present in `orders_tech` and `supplier_deliveries_tech`, padded with a 30-day buffer on each side.

Reasoning: a static hardcoded date range silently stops covering new data the moment source dates move past it, with no error, just missing calendar joins. Deriving the range from live source data means the dimension grows with the data instead of needing manual extension. The buffer exists as insurance against timing gaps between a rebuild and new fact data arriving slightly outside the last calculated bound, not because of any stated business requirement for extra padding.

## Known limitations (deliberate, not oversights)

- **Soft/hard deletes are not tracked in bronze.** Source tables have an `is_active` flag that could support deletion tracking, but wiring it up (`deletion_condition`) requires Databricks Asset Bundles or a direct REST API call, since it is not exposed in the ingestion UI. Deferred given the project timeline. Bronze will silently retain deleted source rows until this is implemented.
- **Query-based connector captures latest state only per run**, not full change history. This has direct downstream impact on SCD2 snapshot completeness and is addressed explicitly in the snapshot design, not ignored.
- **No employee-to-order relationship exists in the source data.** Employee-level reporting is answerable at the store level (staffing, tenure, role history per store) but not at the individual order level, and the model layer reflects that honestly instead of fabricating a join.
- **No true business order date exists in the source system.** Only audit timestamps (`created_at`, `updated_at`, `processed_at`) are available. `created_at` is used as a stated proxy for order date in `fact_orders`. If `created_at` lags the actual order event, downstream time-based reporting inherits that lag. Documented, not discovered live.
- **Supplier margin (procurement cost vs. sale price) is an approximate, downstream-derived metric.** There is no lot or batch traceability linking a specific delivery to the specific units later sold, so margin cannot be computed as a direct per-unit join between the two facts. Any margin reporting built on top of this model is a proximity-based approximation, stated as such.
- **No environment split yet** (dev/staging/prod) in either `profiles.yml` or the Airflow Connection. One target, one connection. Planned as part of the CI phase, since CI needs a stateless runner profile regardless.
- **No CI, no failure alerting yet.** Listed here instead of silently absent from a features list. Scoped as the next phase of work.

## Current build state

- Postgres source (Ghost.build) provisioned, schema loaded: 6 tables (customers, stores, products, employees, orders, order_items).
- Databricks Free Edition constraints verified. Query-based connector confirmed as the correct approach for this tier.
- Bronze ingestion built: query-based incremental connector, cursor column + primary key configured per table, for the primary Postgres source.
- Secondary bronze ingestion built: S3 supplier delivery feed via notebook-driven Auto Loader, scheduled daily.
- dbt project initialized, Databricks adapter connected.
- Silver technical layer complete: all source tables modeled as incremental models with merge strategy, including the supplier delivery feed (`supplier_deliveries_tech`).
- Silver business layer complete: metadata-driven `obt_business` built, verified correct on grain after the employee join fix documented above.
- Generic and singular dbt tests in place across silver technical, business, and gold layers, including direct row count grain checks (not just source-relative checks).
- SCD Type 2 snapshots complete for the Sales-side dimensions (customers, products, stores, employees, orders), verified correct on a second run (no duplication, `dbt_valid_to` sentinel populates correctly on superseded rows).
- Galaxy schema gold layer designed and built: `dim_product` (shared, dual resolution strategy), `dim_supplier` (SCD1, no surrogate key), `dim_date` (conformed, dynamically ranged), `fact_orders` (Sales), `fact_supplier_deliveries` (Procurement).
- Airflow orchestration built and functioning in Docker: DAG sequences the full pipeline, Databricks Job triggered and polled via SDK before any downstream dbt task runs, credentials handled through an Airflow Connection, dbt isolated in its own virtual environment.
- **Active defect, not yet resolved:** the most recent full `dbt test` run (72 tests) surfaced 8 failures concentrated in the gold layer: a `dim_date` schema/model column name mismatch, and a large volume of unresolved SCD2 surrogate keys plus duplicate grain in `fact_orders` and `fact_supplier_deliveries`. Root cause investigation in progress. Full failure detail and fix log in `dbt/README.md` and `DECISION_LOG.md` as it's resolved. This is stated here on purpose, a green test run is not treated as proof of correctness anywhere in this project, and neither is a red one hidden from the README.
- Not yet built: GitHub Actions CI, dev/staging/prod environment parameterization.

This section stays accurate to what actually exists and what is actually verified, not what is designed or intended. A model existing and passing `dbt run` is not the same claim as a model being verified correct, and this README does not conflate the two.

## What I would change with more time

- Add a true incremental state-based selection (`state:modified+`) to the Airflow DAG so a full model set doesn't rebuild on every run regardless of what changed upstream.
- Add async/deferred polling for the Databricks job trigger instead of a synchronous poll holding a worker slot for the full ingestion duration.
- Reconcile `eph_employees` sourcing directly from `employees_tech` instead of `obt_business`, currently a deliberate but unclosed inconsistency versus the other four Sales-side ephemeral models.
- Add persisted `manifest.json`/`run_results.json` artifacts between Airflow runs for real historical run comparison, not just point-in-time Airflow UI visibility.

## Repo structure

```
retail-lakehouse-pipeline/
├── README.md
├── .gitignore
├── dbt/
│   ├── models/
│   │   ├── source/
│   │   ├── silver_tech/
│   │   ├── silver_business/
│   │   └── gold/
│   │       ├── ephemeral/
│   │       ├── dimension/
│   │       └── fact/
│   ├── macros/
│   │   └── custom_schema.sql
│   ├── snapshots/
│   ├── tests/
│   ├── dbt_project.yml
│   └── README.md
├── airflow/
│   ├── dags/
│   ├── docker-compose.yml
│   ├── Dockerfile
│   └── README.md
├── docs/
│   ├── architecture.md
│   └── data_dictionary.md
├── dataset/
│   ├── Data/CSVs
│   └── ddl/walmart_schema.sql
├── S3/
│   └── (supplier delivery synthetic monthly CSVs, Auto Loader notebook)
└── .github/
    └── workflows/          (CI, next phase)
```

## About

End-to-end lakehouse pipeline for retail data: Postgres to Databricks (query-based incremental ingestion), dbt (incremental models, SCD2, metadata-driven OBT, galaxy schema gold layer), Airflow orchestration, S3 secondary ingestion via Auto Loader, CI-gated on dbt tests planned as final phase.
