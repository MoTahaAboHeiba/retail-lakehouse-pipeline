# Retail Lakehouse Pipeline

![Python](https://img.shields.io/badge/python-3.12%2B-blue.svg)
![dbt](https://img.shields.io/badge/dbt-core-orange.svg)
![Databricks](https://img.shields.io/badge/databricks-lakehouse-red.svg)
![Airflow](https://img.shields.io/badge/airflow-orchestration-017CEE.svg)
![PostgreSQL](https://img.shields.io/badge/postgresql-16%2B-336791.svg)
![CI](https://github.com/MoTahaAboHeiba/retail-lakehouse-pipeline/actions/workflows/dbt-ci.yml/badge.svg)

End-to-end data engineering pipeline for retail data. Postgres OLTP source, Databricks lakehouse (bronze/silver/gold), dbt for transformation and testing, Airflow for orchestration, AWS S3 as a secondary ingestion path via Auto Loader, CI-gated on dbt tests through GitHub Actions.

**Status: core pipeline built end to end, gold layer under active data validation.** Bronze through silver is complete and verified. Both gold fact tables are built and orchestrated, but the most recent full test run surfaced integrity defects in both. Root cause investigation is in progress, tracked in [`dbt/README.md`](dbt/README.md) and `DECISION_LOG.md`.

For the full engineering reasoning behind each layer:

- [`dataset/README.md`](dataset/README.md), the Postgres source on Ghost.build
- [`dbt/README.md`](dbt/README.md), transformation layer, the fan-out bug, SCD2 design, gold layer defect log
- [`airflow/README.md`](airflow/README.md), orchestration, Docker issues found and fixed, credential handling
- [`S3/README.md`](S3/README.md), Auto Loader ingestion, why this path exists

This file stays high level. The subsystem READMEs carry the depth.

---

## Table of contents

- [Why this project exists](#why-this-project-exists)
- [Architecture](#architecture)
- [Tech stack and why each piece is there](#tech-stack-and-why-each-piece-is-there)
- [How to run this](#how-to-run-this)
- [Pipeline proof](#pipeline-proof)
- [Important: ingestion pattern is not CDC](#important-ingestion-pattern-is-not-cdc)
- [Engineering decisions](#engineering-decisions)
- [Repo structure](#repo-structure)
- [Current build state](#current-build-state)
- [Known limitations](#known-limitations)
- [What I would change with more time](#what-i-would-change-with-more-time)


---

## Why this project exists

I built this to close two gaps in my own skill set: dbt (incremental models, snapshots, metadata-driven transformation) and Docker (containerized orchestration). Databricks/Spark and Airflow fundamentals I already had, so time here is weighted toward what's actually new to me.

This is not a from-scratch architecture idea. It's based on a published retail data engineering tutorial (Postgres to Databricks to dbt to Airflow), and I'm not pretending otherwise. What I control is the engineering decisions on top of that structure, documented here honestly, including every tradeoff, accepted limitation, and bug I found and fixed.

---

## Architecture

```
Postgres (OLTP source)
    -> Databricks bronze (query-based incremental ingestion via Lakeflow Connect)
    -> AWS S3 (secondary ingestion path, Auto Loader) -> Databricks bronze
    -> dbt silver technical layer (per-table incremental models)
    -> dbt silver business layer (metadata-driven One Big Table)
    -> dbt tests (generic + singular + ci)
    -> dbt snapshots (SCD Type 2 dimension history)
    -> dbt gold layer (galaxy schema: two business processes, shared conformed dimensions)
    -> Airflow (Docker) orchestrates the chain end to end
    -> GitHub Actions CI gates every push on dbt test results
```

The gold layer is a galaxy schema, not a single star schema. Two business processes, Sales and Procurement, share conformed dimensions (`dim_product`, `dim_date`) while each keeps its own fact table at its own grain. Full reasoning in [`dbt/README.md`](dbt/README.md).

## Tech stack and why each piece is there

| Tool | Role | Why |
|---|---|---|
| Databricks (Lakeflow Connect, serverless) | Bronze ingestion, primary source | Query-based incremental load, cursor column plus primary key. Not CDC, see below. |
| Databricks Auto Loader | Bronze ingestion, secondary source | S3 supplier delivery feed. Chosen after `QUOTA_EXCEEDED_EXCEPTION` on the Free Edition ingestion UI, single active pipeline limit. |
| dbt Core + dbt-databricks | Silver/gold transformation | Incremental models, SCD2 snapshots, metadata-driven OBT, galaxy schema gold layer. |
| Airflow (Docker) | Orchestration | Triggers dbt runs and Databricks jobs. No transformation logic inside a DAG task. |
| AWS S3 | Secondary ingestion path | Second source system feeding the same lakehouse, built and scheduled. |
| GitHub Actions | CI | Runs the full dbt test suite on every push, blocks merge on failure. Built and passing. |

## How to run this

Four stages, fixed order. Each stage owns its own setup, linked below.

**Prerequisites:** Python 3.12+, [uv](https://docs.astral.sh/uv/), a Databricks workspace, a PostgreSQL-compatible database ([Ghost.build](https://ghost.build)), Docker.

### 1. Provision the source database

Full steps: [`dataset/README.md`](dataset/README.md)

```
cd dataset
uv venv && source .venv/bin/activate
uv sync
cp .env.example .env          # fill in your Postgres connection string
python setup_db.py            # creates the raw schema, 6 tables
python load_data.py           # loads all 6 CSVs, idempotent
```

### 2. Connect Databricks to both sources

Primary path: query-based Lakeflow Connect against Postgres from step 1. Secondary path: Auto Loader against the S3 supplier feed. Full steps: [`S3/README.md`](S3/README.md)

### 3. Build the transformation layer

Full steps: [`dbt/README.md`](dbt/README.md)

```
cd dbt
dbt deps --project-dir . --profiles-dir .
dbt run --project-dir . --profiles-dir .
dbt test --project-dir . --profiles-dir .
dbt snapshot --project-dir . --profiles-dir .
```

### 4. Orchestrate the full chain

Full steps: [`airflow/README.md`](airflow/README.md)

```
cd airflow
cp .env.example .env          # fill in Databricks credentials
docker compose up -d
```

Open the Airflow UI at `localhost:8080` and trigger `orchestrate.py`.

### Can you run this yourself

Only with your own Databricks workspace and Postgres instance, this project doesn't ship a public demo environment. The proof section below stands in for a live demo.

## Pipeline proof

### dbt lineage

![dbt data lineage](docs/dbt-data-lineage.jpg)

Full source-to-gold dependency graph from `dbt docs generate`.

### dbt test verification

![dbt test verification](docs/dbt-test-verification.jpg)

Generic tests across all four FK pairs, plus singular grain tests, including the row count check that caught the employee fan-out bug below.

### Airflow orchestration

![Sequential DAG run](docs/sequential-dag-run.jpg)
![Parallel DAG run](docs/parallel-dag-run.jpg)

Enforced sequential dependency between stages, and parallel task execution within a stage where no dependency exists.

### Source database running

![Ghost server running](docs/ghost-server-running.jpg)

Postgres instance on Ghost.build, source layer for the pipeline.

### CI gate passing

![CI test passed](docs/ci-test-passed.jpg)

GitHub Actions runs the full dbt test suite on every push and blocks merge on failure. Workflow: [`.github/workflows/dbt-ci.yml`](.github/workflows/dbt-ci.yml).

## Important: ingestion pattern is not CDC

**Decision:** Bronze ingestion uses Lakeflow Connect's query-based connector, cursor column plus primary key per table, not true CDC.

**Reasoning:** Databricks Free Edition is serverless-only, and true CDC needs a continuous classic-compute gateway to read the write-ahead log, which Free Edition can't provision. The tradeoff: this is scheduled polling, not continuous capture, each run captures only the latest row state. Accounted for explicitly in the snapshot design, not discovered late.

---

## Engineering decisions

### Metadata-driven silver business layer

**Decision:** `obt_business` is built from a structured config (table ref, join key, columns) compiled into SELECT and JOIN clauses by a Jinja for-loop, not hand-written SQL.

**Reasoning:** Adding a source table means one config entry, not new SQL re-verified by inspection. The config uses `ref()`, not hardcoded paths, so `dbt docs generate` lineage survives the abstraction. Full writeup in [`dbt/README.md`](dbt/README.md).

### Found and fixed a 10x row inflation bug via independent verification

**What happened:** `obt_business` produced 300,513 rows against an expected grain of 30,021. Every `dbt run` and `dbt test` passed clean, none of it caught this.

**Root cause:** `employees` was joined into the OBT on `store_id`, not unique on that table, a store has many employees, so the join cross-producted. Deeper cause: `orders` has no `employee_id`, the source system never recorded which employee handled which order.

**Fix:** removed the join instead of approximating one the data doesn't support. Employee stays its own dimension, connected to store in gold, not to the fact table. This is what led to the standing practice below, and it's also what caught the active gold layer defect noted in Current Build State. Full detail in [`dbt/README.md`](dbt/README.md) and `DECISION_LOG.md`.

**Standing practice:** every new or changed join gets a row count check against its expected grain before being called done. A green `dbt run` confirms execution, not correctness.

### `dim_product`, shared but not fully conformed

**Decision:** Sales resolves `dim_product` point-in-time via SCD2. Procurement resolves to the current record only.

**Reasoning:** A supplier delivery is a procurement transaction, not a product history event. Treating procurement cost changes as SCD2-worthy product history would conflate two different questions, what a product is, versus what it cost to acquire, into one timeline.

### `dim_supplier`, SCD1, no surrogate key

**Decision:** Supplier attributes are treated as low-volatility reference data. `fact_supplier_deliveries` joins on the natural key `supplier_id` directly, no surrogate key layer.

**Reasoning:** No stated business requirement exists to track supplier name history. Adding SCD2 machinery to a two-column reference dimension is complexity without a requirement behind it.

### `dim_date`, dynamically generated

**Decision:** `dim_date` is shared across both facts, its range derived at build time from the min/max dates in the source tables, padded 30 days on each side.

**Reasoning:** A hardcoded range silently stops covering new data with no error, just missing joins. Deriving it from live source data means the dimension grows with the data instead of needing manual extension.

---

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
│   │       ├── ci/
│   │       ├── grain/
│   │       └── singular/
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
│   ├── ddl/walmart_schema.sql
│   └── README.md
├── S3/
│   ├── (supplier delivery synthetic monthly CSVs)
│   └── README.md
└── .github/
    └── workflows/
        └── dbt-ci.yml
```

---

## Current build state

- Postgres source provisioned, 6 tables loaded (customers, stores, products, employees, orders, order_items).
- Primary bronze ingestion built: query-based connector, cursor column plus primary key per table.
- Secondary bronze ingestion built: S3 supplier feed via external location, scheduled upsert into a bronze streaming table.
- Silver technical layer complete: all source tables incremental, including `supplier_deliveries_tech`.
- Silver business layer complete: `obt_business` verified correct on grain after the fan-out fix.
- Galaxy schema gold layer built: two independent business processes, shared conformed dimensions only where the relationship is real.
  - `fact_orders` (sales, order line grain) and `fact_supplier_deliveries` (procurement, delivery line grain).
  - `dim_date` and `dim_products_current` shared across both; `dim_supplier` (SCD1) and SCD2 dimensions scoped to their owning process. Full reasoning in [`dbt/gold/README.md`](dbt/gold/README.md).
- SCD2 snapshots complete for Sales-side dimensions, verified correct on a second run. `dim_supplier` deliberately SCD1, no snapshot, no stated business requirement to track history on reference data.
- Generic and singular tests in place across silver and gold, including direct row count grain checks per fact table.
- Airflow orchestration built and functioning in Docker, full DAG sequencing, credentials via Airflow Connection.
- GitHub Actions CI built and passing: dbt tests run on every push, blocks merge on failure.
- Not yet built: dev/staging/prod parameterization, Power BI reporting layer (in progress, three-page report planned: Sales, Procurement, Workforce).

---

## Known limitations

- **Soft/hard deletes aren't tracked in bronze.** An `is_active` flag could support it, wiring it up needs Databricks Asset Bundles or a direct REST call, not exposed in the ingestion UI. Deferred.
- **Query-based connector captures latest state only per run**, not full change history. Addressed in the snapshot design, not ignored.
- **No employee-to-order relationship exists in the source data.** Employee reporting is answerable at the store level, not per order.
- **No true business order date exists.** `created_at` is used as a stated proxy for order date in `fact_orders`. If it lags the real event, downstream time-based reporting inherits that lag.
- **Supplier margin is an approximate, downstream-derived metric.** No lot or batch traceability links a specific delivery to the units later sold, so margin is a proximity-based approximation, stated as such.
- **No environment split yet** (dev/staging/prod). One target, one connection.
- **No failure alerting yet.** CI checks run and block merge, but a failure is visible only in the Actions tab, not pushed anywhere.


## What I would change with more time

- Persist `manifest.json`/`run_results.json` between Airflow runs for real historical run comparison, not just point-in-time Airflow UI visibility.
