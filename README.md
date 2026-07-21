# Retail Lakehouse Pipeline

End-to-end data engineering pipeline for retail data. Postgres OLTP source, Databricks lakehouse (bronze/silver/gold), dbt for transformation and testing, Airflow for orchestration, S3 as a secondary ingestion path, CI gating on dbt tests.

**Status: in progress.** This README reflects the current build state, not a finished system. I update it as each layer gets built, not retroactively at the end.

For the full engineering reasoning behind each layer, see:
- [`dataset/README.md`](./dataset/README.md), psotgreSQL Database on ghost.build, why, and how to use it
- [`dbt/README.md`](./dbt/README.md), transformation layer decisions, the fan-out bug, testing strategy, SCD2 design
- [`airflow/README.md`](./airflow/README.md), orchestration decisions, Docker issues found and fixed, credential handling

This file stays high-level. The subsystem READMEs carry the depth.

---

## Why this project exists

I built this to go deep on two areas I identified as gaps in my own skill set: dbt (incremental models, snapshots, metadata-driven transformation) and Docker (containerized orchestration). Databricks/Spark and Airflow fundamentals I already had going in, so time here is weighted toward the parts that are actually new to me.

This is not a from-scratch architecture idea. It is based on a published retail data engineering tutorial (Postgres to Databricks to dbt to Airflow). I am not pretending otherwise. What I control is the engineering decisions on top of that structure, and this README documents those decisions honestly, including the ones that involved a tradeoff, a limitation I chose to accept, or a bug I found and fixed.

## Architecture

```
Postgres (OLTP source)
    -> Databricks bronze (query-based incremental ingestion via Lakeflow Connect)
    -> dbt silver technical layer (per-table incremental models)
    -> dbt silver business layer (metadata-driven One Big Table)
    -> dbt tests (generic + singular)
    -> dbt snapshots (SCD Type 2 dimension history)
    -> dbt gold layer (star schema fact table)
    -> Airflow (Docker) orchestrates the chain end to end
    -> AWS S3 as a secondary raw ingestion path
    -> GitHub Actions CI gates every push on dbt test results
```

A full architecture diagram will replace this block once the gold layer and CI are both closed.

## Tech stack and why each piece is there

| Tool | Role | Why |
|---|---|---|
| Databricks (Lakeflow Connect, serverless) | Bronze ingestion | Query-based incremental load using cursor column + primary key. See ingestion note below, this is deliberately not labeled CDC. |
| dbt Core + dbt-databricks | Silver/gold transformation | Incremental models, SCD2 snapshots, metadata-driven OBT via Jinja. |
| Airflow (Docker) | Orchestration | Triggers dbt runs and Databricks jobs. No transformation logic lives inside a DAG task. |
| AWS S3 | Secondary ingestion path | Represents a second source system feeding the same lakehouse, separate from the live OLTP path. Not yet built. |
| GitHub Actions | CI | Runs dbt tests on every push, blocks merge on failure. Not yet built. |

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

Standing practice this created: every new or changed join gets a row count check against its expected grain before being considered done. A green `dbt run` confirms the model executed. It does not confirm the numbers are right.

## Known limitations (deliberate, not oversights)

- **Soft/hard deletes are not tracked in bronze.** Source tables have an `is_active` flag that could support deletion tracking, but wiring it up (`deletion_condition`) requires Databricks Asset Bundles or a direct REST API call, since it is not exposed in the ingestion UI. Deferred given the project timeline. Bronze will silently retain deleted source rows until this is implemented.
- **Query-based connector captures latest state only per run**, not full change history. This has direct downstream impact on SCD2 snapshot completeness and is addressed explicitly in the snapshot design, not ignored.
- **No employee-to-order relationship exists in the source data.** Employee-level reporting is answerable at the store level (staffing, tenure, role history per store) but not at the individual order level, and the model layer reflects that honestly instead of fabricating a join.
- **No environment split yet** (dev/staging/prod) in either `profiles.yml` or the Airflow Connection. One target, one connection. Planned before CI is wired up, since CI needs a stateless runner profile regardless.
- **No CI, no failure alerting, no S3 ingestion path yet.** Listed here instead of silently absent from a features list.

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
└── .github/
    └── workflows/
```

## Current build state

- Postgres source (Ghost.build) provisioned, schema loaded: 6 tables (customers, stores, products, employees, orders, order_items).
- Databricks Free Edition constraints verified. Query-based connector confirmed as the correct approach for this tier.
- Bronze ingestion built: query-based incremental connector, cursor column + primary key configured per table.
- dbt project initialized (`first_dbt_project_walmart`), Databricks adapter connected.
- Silver technical layer complete: all six source tables modeled as incremental models with merge strategy.
- Silver business layer complete: metadata-driven `obt_business` built, verified correct on grain after the employee join fix documented above.
- Generic and singular dbt tests in place across silver technical, business, and fact layers, including direct row count grain checks (not just source-relative checks).
- SCD Type 2 snapshots complete for all five dimensions, verified correct on a second run (no duplication, `dbt_valid_to` populates correctly on superseded rows).
- Airflow orchestration built and functioning in Docker: DAG sequences the full pipeline (ingestion trigger through gold fact), Databricks Job triggered and polled via SDK before any downstream dbt task runs, credentials handled through an Airflow Connection, dbt isolated in its own virtual environment.
- **Gold star schema fact table: in progress.** Point-in-time SCD2 join pattern (business key match plus `order_date BETWEEN dbt_valid_from AND dbt_valid_to`) identified as the correct approach, not yet implemented.
- Not yet built: GitHub Actions CI, AWS S3 secondary ingestion, environment (dev/staging/prod) parameterization.

This section gets replaced by a finished feature list once the build is complete. Until then it stays accurate to what actually exists, not what is planned.

## What I would change with more time

To be added as each build phase surfaces a real tradeoff, not written speculatively in advance.
