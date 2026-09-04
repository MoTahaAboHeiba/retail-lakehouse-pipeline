# PostgreSQL Dataset

Here is how I organized the PostgreSQL source data for the incremental ingestion tests.

I split the data into **5 batches** to simulate how a real retail system grows and changes over time:

* **Batch000**: Small baseline dataset to kick off the pipeline.
* **Batch001**: Large incremental workload. The repository contains a sample.
* **Batch002**: Large incremental workload. The repository contains a sample.
* **Batch003**: Large incremental workload. The repository contains a sample.
* **Batch004**: Large incremental workload. The repository contains a sample.

The complete datasets are intentionally not stored in GitHub because of their size. I kept representative samples here so you can still inspect the actual data and understand the structure.

The batches simulate **new records and updates to existing records**, using the primary key and `updated_timestamp` for incremental ingestion.

```text
Batch000 → Batch001 → Batch002 → Batch003 → Batch004
 Baseline       Incremental Changes
```
