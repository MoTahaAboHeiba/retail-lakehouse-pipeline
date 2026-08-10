# Decision Log: Walmart DE Project

Format: Date - Decision - Why (engineering) - Why (business impact)
Append future entries below

---

## 2026-07-13 - Refactored `obt_business.sql` to be genuinely metadata-driven

**What changed:** Replaced hardcoded schema paths (`walmart.silver_tech.orders_tech`) with `ref()`-based lookups inside a structured config. Replaced pre-written SQL column string blobs with structured column dictionaries (`{"col": ..., "as": ...}`) consumed by a Jinja for-loop that generates the SELECT and JOIN clauses at compile time.

**Engineering reasoning:** Hardcoded paths break dbt's dependency graph, `dbt docs generate` cannot trace lineage through a model that doesn't use `ref()`. A config where the table name is still a hardcoded string is not metadata-driven, it just relocates the same defect into a dict. True metadata-driven design means the config is data (table ref name, alias, join key, column list) and the SQL is a generic engine that compiles from it. Adding a 7th source table now means adding one config entry, not writing a new SQL block.

**Business framing:** The pipeline that assembles order, customer, and product data into one usable table is now built so new data sources can be added without rewriting core logic, reducing future engineering time and the risk of inconsistent changes across tables.

---

## 2026-07-14 - Discovered and corrected a 10x row inflation bug from the employee join

**What was found:** `obt_business` row count was 300,513 against an expected ~30,021 (the `order_items_tech` grain). Root cause: `employees_tech` was joined on `store_id`, which is not a unique key on that table (a store has many employees). This produced a cross-product between order lines and every employee at that store. The same defect had already propagated into `fact_orders` via `ref()` lineage, undetected, because every prior `dbt run` and `dbt test` passed with no errors.

**Deeper root cause:** `orders_tech` has no `employee_id` column at all. The source system does not record which specific employee handled a given order. There is no accurate way to attribute an order to an employee with current source data, joining on `store_id` was an attempt to approximate a relationship that doesn't exist.

**Decision:** Remove the employee join from both `obt_business` and `fact_orders`. Do not attempt a workaround (e.g. picking one employee arbitrarily per store), as that would fabricate a relationship the data does not support. `employees_tech` is not dropped, it remains a valid source table and continues to feed the existing `eph_employees` ephemeral model as its own dimension.

**Engineering reasoning:** A foreign key on a fact table must represent a real relationship at that grain. Employee has no transactional relationship to an individual order in this data, but it does have a real one-to-one-per-employee relationship to a store. Verification discipline mattered here: a clean `dbt run` and passing tests did not catch this, only an independent row count check against the known source grain did. This is the standing practice going forward, every new or changed join gets a row count sanity check against its expected grain before being considered done.

**Business framing:** The orders table was showing an employee ID field that implied "the employee who handled this order." In reality it was listing every employee who has ever worked at that store, repeated across every order line, inflating counts roughly tenfold and making any employee-based analysis on this table unreliable. This was corrected before external use. Employee-level reporting will be delivered through a proper staffing dimension instead, once built, which answers "who works at this store" accurately, rather than a fabricated "who handled this order" that the source data cannot support.

**Stakeholder communication (sent before implementation, not after):**
> The employee ID field on the orders table was being calculated using a join on store ID. Since each store has multiple employees, this meant every order line was being duplicated once per employee at that store, inflating row count roughly tenfold and making any employee-based analysis on this table unreliable. After investigating the source system, there is no field recording which specific employee handled a given order, so there is no accurate way to fix this join as it stood, the relationship does not exist in the data. We are removing employee ID from the orders table entirely. Employee-level reporting going forward will come from a separate staffing dimension reflecting who works at each store, not who handled which order.

---

## 2026-07-14 - Employee dimension modeled under the store dimension, not the fact table

**Decision:** In the gold star schema, `employee` connects to the `store` dimension (snowflake pattern), not directly to `fact_orders`. Employee is also planned as its own SCD Type 2 history, tracked independently from the store's SCD2 history.

**Engineering reasoning:** The fact table's grain is order line item; a dimension only belongs there if it has a real relationship at that grain. Store does (one order happens at exactly one store). Employee does not (no order-level employee record exists). Employee's real relationship is to store, not to order, so it hangs off the store dimension instead. Employees change roles, get promoted, transfer stores, or are terminated, so SCD2 is needed to preserve staffing history accurately over time. Because store and employee each change independently and on their own schedules, their SCD2 timelines are kept separate: each dimension is filtered for validity at a point in time using its own `dbt_valid_from`/`dbt_valid_to`, then joined on the natural business key (`store_id`), rather than forcing one shared validity window across both. Coupling their lifecycles into a single timeline would produce point-in-time joins that are technically present but temporally inconsistent.

**Business framing:** Employee-level reporting (headcount, tenure, role history per store) is now modeled correctly as a staffing question, answerable and historically accurate, fully separated from order-level reporting, which the source data cannot support at the employee level.

**Open action:** Confirm this design before Day 8 snapshot build begins, not during it.

---

## Outstanding items not yet logged as decisions (tracked for follow-up)
- `deletion_condition` soft-delete config deferral (already logged separately, referenced here for consistency of format)
- Day 1 and Day 2 verbal checkpoints still owed
- Full `PROJECT_TRACKER.md` correction pass for Days 3-5, not yet done


## 2026-07-17 - Added direct grain test on fact_orders, closing a downstream test coverage gap

**What changed:** Added `fact_orders_grain.sql`, a singular test comparing `fact_orders` row count directly against `order_items_tech`, the same pattern used for `obt_business_grain`. Also added `not_null` on all five key columns and `unique` on `order_item_id` via a new `models/gold/fact/schema.yml`.

**Engineering reasoning:** `obt_business_grain` verifies correctness at the silver_business layer. It does not verify `fact_orders`, a separate model one layer downstream. Today `fact_orders` is a pure passthrough SELECT with no join or filter logic, so it inherits obt_business's correctness for free, but that is an accident of its current simplicity, not a guarantee. The moment gold-layer logic is added to `fact_orders` (planned for Day 9), the upstream test stops being a valid proxy for its correctness. Test coverage that lives one layer upstream of where a defect can be introduced is not coverage, it's coverage for a version of the model that no longer exists once the model changes. This closes that gap before it becomes live risk, rather than after.

**Business framing:** The same failure mode that caused the 10x employee fan-out bug, a transformation with no test verifying its own output, cannot recur silently at the fact table layer. Every model that ships to reporting now has a direct grain check tied to its own output, not just to its source.

---

## 2026-08-01 - Fixed dimension ephemeral model sourcing: moved from obt_business to _tech tables

**What was found:** Full `dbt test` run (72 tests) returned 8 failures concentrated in the gold layer, most severely `unique_fact_orders_order_item_id` (20,078 duplicates out of a 30,021-row grain) and mass `not_null` failures across all four SCD2 surrogate keys on `fact_orders` (16,189 / 15,690 / 8,517 / 13,069 nulls). Independent diagnostic query confirmed `dim_orders` was carrying 2 to 5 "active" (`dbt_valid_to = 9999-12-31`) versions per `order_id`, when exactly one is correct. `dim_customers`, `dim_products`, and `dim_stores` showed no duplicate active versions at the time of that check.

**Root cause:** All four dimension ephemeral models (`eph_customers`, `eph_products`, `eph_stores`, `eph_orders`) were sourcing from `obt_business`, a line-item-grain table, instead of their matching business-key-grain `_tech` table. Each model attempted to collapse back down to dimension grain using `SELECT DISTINCT` across a column list that included a `_processed_at` timestamp (varies per incremental batch) and `CURRENT_TIMESTAMP()` (varies per query execution, non-deterministic across rows). `DISTINCT` cannot deduplicate when one of its columns is guaranteed to differ row to row, it silently becomes a no-op. On `eph_orders`, an order's line items processed across different incremental batches produced multiple near-identical rows per `order_id`, which the snapshot then captured as multiple overlapping "current" versions, causing fanout in every fact join against `dim_orders`.

**Decision:** Ephemeral dimension models source directly from their matching `_tech` table, not from `obt_business`. Processing-metadata columns (`_processed_at`, `CURRENT_TIMESTAMP()` aliases) are dropped from all four ephemeral models entirely, they are pipeline metadata, not dimension attributes, and have no legitimate role in an SCD2 change-detection surface. `obt_business` remains scoped to feeding `fact_orders` only, where its line-item grain is actually required.

**Engineering reasoning:** Every unnecessary grain change is a place a bug can hide. `_tech` tables are already deduplicated to one row per business key via the Day 4 standardization pattern (`row_number() partition by <pk> order by updated_timestamp desc`, `rn = 1`), grain-verified independently at the time. Routing dimension prep through OBT and back down via `DISTINCT` reintroduced a grain-collapse step that didn't need to exist, and that step broke silently, the same failure shape as the Day 4 employee fan-out bug: a join or dedup key assumed unique that wasn't. This is now a standing pattern to watch for, any `DISTINCT` or `GROUP BY` used for deduplication must be checked against every column in its own list for row-level nondeterminism, not just against the intended business key.

**Verification:** Snapshot tables were dropped and rebuilt (`dbt snapshot --full-refresh` is not supported on dbt Core 1.11, used manual `DROP TABLE` + `dbt snapshot` instead). Post-fix, all four dimensions independently confirmed zero duplicate active versions per business key via direct query, not inferred from a passing test.

**Business framing:** A reporting table that silently fans out order lines against a broken orders dimension would have overstated order volume and corrupted any downstream count, sum, or customer-level rollup built on top of it, without a single error or warning surfacing anywhere in the pipeline. Caught and fixed before the table was used for anything, through the same independent-verification discipline established after the employee fan-out incident: a green test suite is not proof of correctness, only independent grain and uniqueness checks are.

---

## 2026-08-03 - Added SCD2 floor-gap fallback join to fact_orders, replacing strict BETWEEN

**What was found:** After fixing the ephemeral sourcing bug above, `unique_fact_orders_order_item_id` passed (fanout eliminated), but `not_null` failures persisted on all four surrogate keys at reduced but still substantial counts: 8,891 (customer), 8,637 (product), 7,739 (store), 8,517 (order), against a 30,021-row grain.

**Root cause:** `fact_orders` resolves each dimension using `ob.order_timestamp BETWEEN dc.dbt_valid_from AND dc.dbt_valid_to`. `dbt_valid_from` on a timestamp-strategy snapshot is set from the source's `updated_at` column, which reflects when that row was first observed by the snapshot, not when the underlying entity actually came into existence. A customer, product, store, or order whose only captured dimension version has an `updated_at` later than some of that same key's own order history produces a real coverage gap: the order predates the earliest known state of its own dimension. A strict `BETWEEN` silently drops any row that falls in this gap, no error, no warning.

An earlier global floor check (comparing the single earliest order timestamp against the single earliest dimension version, aggregated across the whole table) understated this problem by roughly 60x, since it only catches the case where the table-wide minimum dimension version postdates the table-wide minimum order. It missed the real mechanism: individual business keys, scattered throughout the dataset, each having their own personal floor later than their own personal order history, even while the aggregate minimums looked closely aligned.

**Verification before fix:** For each dimension, confirmed with a direct per-key floor query that null count exactly matched the count of orders whose timestamp fell before that specific key's earliest `dbt_valid_from`: customer 8,891/8,891, product 8,637/8,637, store 7,739/7,739, order 8,517/8,517. All four confirmed exact before any code was changed.

**Decision:** All four dimension joins in `fact_orders.sql` were changed from strict `BETWEEN` to a `BETWEEN OR fallback-to-earliest-version` pattern: if an order's timestamp falls inside a real captured validity window, join normally; if it predates a key's earliest known version, join to that earliest version instead of dropping the row. Each dimension's per-key earliest `dbt_valid_from` is precomputed as a CTE (`MIN(dbt_valid_from) GROUP BY <key>`), not a correlated scalar subquery inside the JOIN predicate, since Spark's Catalyst optimizer does not support correlated scalar subqueries inside JOIN conditions (`UNSUPPORTED_CORRELATED_SCALAR_SUBQUERY`), confirmed by a direct build failure on the first implementation attempt.

**Engineering reasoning:** SCD2 tracking cannot recover an entity's attribute state from before tracking began, that's an inherent limitation of the pattern, not a bug to hide. Silently dropping the row is the wrong response to that limitation, since it understates fact table volume with no visibility into why. Resolving to the earliest known version is the standard, defensible response: it's the closest available truth. The precomputed-CTE implementation was chosen over the correlated subquery not just because the subquery failed to compile, but because it's the correct approach at any real scale, a CTE with a `GROUP BY` runs once per dimension; a correlated subquery risks re-evaluating per row.

**Business framing:** Without this fix, roughly 30% of order line items would have been invisible to any report joining through customer, product, store, or order dimension attributes, with no error surfaced anywhere, only a passing `dbt run`. The fix ensures every order line resolves to a dimension state, defaulting to the earliest known version when an order predates all captured history, and that limitation is now documented rather than silently absorbed as data loss.


---

## Gold layer test suite status

As of 2026-08-06, full `dbt test` run: 72/72 pass. All fixes above independently verified against row counts, grain checks, and null counts directly, not accepted on a passing test suite alone. Day 9 checkpoint closed.
