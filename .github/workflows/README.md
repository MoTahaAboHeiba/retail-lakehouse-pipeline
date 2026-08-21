# CI: dbt Test Gate

## What this does

`dbt-ci.yml` runs on every pull request and every push to `main`. It runs the tests tagged `ci` and fails the job on the first failure. A red run blocks merge.

## Why `tag:ci`, not the full test suite

**Decision:** The test step selects `--select "tag:ci"`, not every test in the project.

**Reasoning:** `tests/ci/` (`ci_quality_checks.sql`, `ci_quality_gate.sql`) holds the checks scoped to gate a push, fast and deterministic. Grain tests (`tests/grain/`) and join-specific singular tests (`tests/singular/`) run as part of the full pipeline pass orchestrated by Airflow, not on every push.

## Credentials

Databricks host, HTTP path, and token are stored as GitHub Actions secrets (`DBT_HOST`, `DBT_HTTP_PATH`, `DBT_TOKEN`), injected as environment variables, never committed. Same pattern as the Airflow Connection: no credential exposure in version control, no environment-specific fork of the workflow file.
