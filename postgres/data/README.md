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
## Dataset

The current PostgreSQL source contains **1,653,490 rows** across 6 OLTP tables:

| Table | Rows |
|---|---:|
| customers | 22,000 |
| products | 600 |
| employees | 450 |
| orders | 407,500 |
| order_items | 1,223,915 |
| stores | 25 |
| **Total** | **1,653,490** |

This workload is large enough to validate the pipeline's incremental ingestion, PK-based upserts, cursor logic, relational integrity, and moderate performance behavior.

It should be considered a **medium-scale synthetic workload**, not a production-scale benchmark.
