# Next moves for the dbt project

## Priority 1 — harden the pipeline for production use
- Move database credentials out of the profile into environment variables or a secrets manager.
- Keep the current explicit silver-layer column contracts and continue using them as the contract boundary between bronze and business logic.

## Priority 2 — strengthen the quality gate
- Keep the CI-style checks for nulls, uniqueness, relationships, and grain reconciliation.
- Add a small set of regression tests around the point-in-time joins in the gold fact model.
- Make the CI workflow run the same checks developers would expect locally before merge.

## Priority 3 — improve maintainability
- Add descriptions to sources, models, and key columns so dbt docs are more useful.
- Review the gold ephemeral models and decide whether they should stay as compile-time helpers or be promoted into real reusable models.
- Validate the snapshot and dimension logic with a few representative change scenarios.

## Priority 4 — prepare for scale
- Revisit incremental behavior once real data volume and late-arriving updates are available.
- Add observability around run duration, row counts, and freshness so regressions are easier to spot.
