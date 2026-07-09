# Retail Lakehouse Pipeline

End-to-end data engineering pipeline for retail data. Postgres OLTP source, Databricks lakehouse (bronze/silver/gold), dbt for transformation and testing, Airflow for orchestration, S3 as a secondary ingestion path, CI gating on dbt tests.

**Status: in progress.** This README reflects the current build state, not a finished system. I am updating it as each layer gets built, not writing it retroactively at the end.

---

## Why this project exists

I built this to go deep on two areas I identified as gaps in my own skill set: dbt (incremental models, snapshots, metadata-driven transformation) and Docker (containerized orchestration). Databricks/Spark and Airflow fundamentals I already had going in, so time here is weighted toward the parts that are actually new to me.

This is not a from-scratch architecture idea. It is based on a published retail data engineering tutorial (Postgres to Databricks to dbt to Airflow). I am not pretending otherwise. What I control is the engineering decisions on top of that structure, and this README documents those decisions honestly, including the ones that involved a tradeoff or a limitation I chose to accept.

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

A full architecture diagram will replace this block once the gold layer is built.

## Tech stack and why each piece is there

| Tool | Role | Why |
|---|---|---|
| Databricks (Lakeflow Connect, serverless) | Bronze ingestion | Query-based incremental load using cursor column + primary key. See ingestion note below, this is deliberately not labeled CDC. |
| dbt Core + dbt-databricks | Silver/gold transformation | Incremental models, SCD2 snapshots, metadata-driven OBT via Jinja. |
| Airflow (Docker) | Orchestration | Triggers dbt runs and Databricks jobs. No transformation logic lives inside a DAG task. |
| AWS S3 | Secondary ingestion path | Represents a second source system feeding the same lakehouse, separate from the live OLTP path. |
| GitHub Actions | CI | Runs dbt tests on every push, blocks merge on failure. |

## Important: ingestion pattern is not CDC

Databricks Free Edition is serverless-only. True CDC through Lakeflow Connect's PostgreSQL connector requires a continuous classic-compute gateway to consume the write-ahead log through logical replication, and Free Edition cannot provision that gateway.

I use Lakeflow Connect's query-based connector instead: a cursor column plus a primary key per table, driving scheduled incremental upserts. This runs fully serverless with no gateway requirement.

The tradeoff I accepted: this is scheduled polling, not continuous capture, and each run captures only the latest row state, not every intermediate change between runs. I account for this explicitly in the snapshot design rather than discovering it as a surprise at the dimension history stage.

I am flagging this here, upfront, because claiming CDC when the mechanism is query-based incremental ingestion is the kind of inaccuracy that does not survive a technical interview.

## Known limitations (deliberate, not oversights)

- **Soft/hard deletes are not tracked in bronze.** Source tables have an `is_active` flag that could support deletion tracking, but wiring it up (`deletion_condition`) requires Databricks Asset Bundles or a direct REST API call, since it is not exposed in the ingestion UI. Deferred given the project timeline. Bronze will silently retain deleted source rows until this is implemented.
- **Query-based connector captures latest state only per run**, not full change history. This has direct downstream impact on SCD2 snapshot completeness and is addressed explicitly in the snapshot design, not ignored.

## Repo structure

```
retail-lakehouse-pipeline/
├── README.md
├── .gitignore
├── dbt_walmart/
│   ├── models/
│   │   ├── source/
│   │   ├── silver_t/
│   │   ├── silver_b/
│   │   └── gold/
│   ├── snapshots/
│   ├── tests/
│   └── dbt_project.yml
├── airflow/
│   ├── dags/
│   ├── docker-compose.yml
│   └── Dockerfile
├── docs/
│   ├── architecture.md
│   └── data_dictionary.md
└── dataset/
│   ├── Data/CSVs
│   └── ddl/walmart_schema.sql
└── .github/
    └── workflows/
```

## Current build state

- Postgres source (Ghost.build) provisioned, schema loaded: 6 tables (customers, stores, products, employees, orders, order_items).
- Databricks Free Edition constraints verified. Query-based connector confirmed as the correct approach for this tier.
- Bronze ingestion built: query-based incremental connector, cursor column + primary key configured per table.
- dbt project initialized, Databricks adapter connected.
- Airflow running locally in Docker.
- Silver layer, snapshots, gold layer, CI, and S3 ingestion: not yet built.

This section will be replaced by a finished feature list once the build is complete. Until then it stays accurate to what actually exists, not what is planned.

## What I would change with more time

To be added as each build phase surfaces a real tradeoff, not written speculatively in advance.
