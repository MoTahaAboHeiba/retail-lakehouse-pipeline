# S3 Secondary Ingestion

## Architecture

**Decision:** A managed Lakeflow ingestion pipeline, configured through Jobs & Pipelines, connected directly to the S3 bucket as its source. Not Auto Loader, not a hand-written external-table read.

**Reasoning:** The managed pipeline handles file detection and the bronze-layer merge internally, no custom PySpark or SQL merge logic was written for this layer. Given the ingestion mechanism itself isn't the skill gap this project was built to close (see main README, Why This Project Exists), using the platform's managed path here was the right scope call over hand-rolling an ingestion job to prove a point.

```text
S3 bucket (monthly supplier_deliveries CSVs)
        │
        ▼
Lakeflow ingestion pipeline (independent schedule, outside Airflow)
        │
        ▼
supplier_deliveries_b (bronze, streaming table, merged by pipeline)
        │
        ▼
supplier_deliveries_tech (dbt silver, incremental, unique_key='delivery_id', strategy='merge')
        │
        ▼
dim_supplier + fact_supplier_deliveries (gold, procurement business process)
```

Two separate merge operations at two separate layers: the Lakeflow pipeline merges new S3 files into the bronze streaming table, internal to the managed pipeline. `supplier_deliveries_tech` is a separate incremental dbt model reading from that bronze table, with its own `unique_key` and merge strategy. Two distinct decisions, not one operation described twice.

## The pipeline runs independently, so Airflow verifies it instead of trusting it

**Decision:** The S3 pipeline runs on its own Databricks-managed schedule, outside Airflow's control. A DAG task polls the Databricks Pipelines API before the entire procurement chain that depends on it, `supplier_deliveries_tech` through `dim_supplier` and `fact_supplier_deliveries`, and raises if the latest pipeline update isn't in a completed, current state.

**Reasoning:** Same principle as the primary ingestion path polling in `airflow/README.md`, don't assume a source landed just because it's scheduled to. A pipeline outside your orchestrator's control is a scheduling accident waiting to become a silent gap unless something explicitly checks its terminal state before downstream work depends on it. Gating the full procurement chain, not just the first silver model, means a stale or failed S3 pipeline can't produce a `fact_supplier_deliveries` that looks complete but is quietly built on old data.

## Known limitations

- **Malformed CSVs aren't handled yet.** No schema validation or bad-row quarantine on the incoming monthly file. A malformed file's behavior today is whatever the managed pipeline's default failure mode is, not something explicitly designed for. Flagged here instead of implying resilience that isn't built.
- **One file per month.** The pipeline re-scans and merges on `delivery_id` rather than depending on new-file detection logic, so cadence is a source-data fact, not an ingestion constraint.
