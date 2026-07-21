# Airflow Orchestration

This directory contains the orchestration layer for the retail lakehouse pipeline.

Airflow does not ingest data and does not transform data. It sequences two systems that already do those jobs correctly on their own: a Databricks Job for ingestion, and dbt for every transformation. The DAG's only responsibility is ordering, dependency enforcement, and failure isolation.

---

## Pipeline Flow

```text
Databricks Ingestion
        │
        ▼
dbt deps
        │
        ▼
Source Freshness
        │
        ▼
Silver Technical
        │
        ▼
Silver Technical Tests
        │
        ▼
Silver Business
        │
        ▼
Silver Business Tests
        │
        ▼
Gold Ephemeral Models
        │
        ▼
Snapshots (SCD Type 2)
        │
        ▼
Gold Fact Models
```

Each arrow is a hard dependency. If a stage fails, nothing downstream of it runs. No dbt task executes against partially ingested or partially tested data.

---

## Project Structure

```text
airflow/
├── dags/
│   └── orchestrate.py
├── config/
├── plugins/
├── .env.example                  # Template — copy to .env and fill credentials
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
└── README.md
```

---

# Engineering Decisions

## Airflow is an orchestrator, not a processing engine

**Decision:** The DAG contains zero SQL and zero transformation logic. It does exactly two things: trigger the Databricks ingestion job, and execute dbt commands in a fixed order.

**Engineering reasoning:** Putting transformation logic inside a DAG task duplicates responsibility that dbt already owns, lineage, dependency resolution, testing, and documentation. A DAG task that runs raw SQL against the warehouse has none of that context and becomes an untracked side door into the data model. Keeping Airflow strictly declarative (when something runs) and dbt strictly transformational (how data changes) means a change to business logic only ever happens in one place.

**Business framing:** Orchestration and transformation are decoupled, so a change to a join or a business rule never requires touching the scheduler, and a change to scheduling never risks touching business logic.

---

## `run` and `test` are separate tasks, not `dbt build`

**Decision:** Every layer executes as two tasks: build, then test. `dbt build` was rejected even though it would collapse this into fewer tasks.

**Engineering reasoning:** `dbt build` reports a single pass/fail for a combined operation. When something fails, I want the Airflow UI to tell me immediately whether the model failed to build or the model built but failed validation, those are different failure classes with different fixes. Collapsing them into one task destroys that signal to save a few DAG nodes.

```text
Silver Technical
        │
        ▼
Silver Technical Tests
        │
        ▼
Silver Business
        │
        ▼
Silver Business Tests
```

**Business framing:** More tasks, faster root cause. A failed pipeline points at exactly which layer and which failure type broke, instead of requiring someone to dig through logs to find out.

---

## Ingestion runs as a monitored Databricks Job, not inline Airflow logic

**Decision:** Bronze ingestion is triggered through the Databricks SDK (`WorkspaceClient`) as an external job. Airflow polls the Jobs API until the run reaches a terminal state before any dbt task is allowed to start.

**Engineering reasoning:** Ingestion is compute-heavy and belongs on the compute engine, not inside a scheduler container. Reimplementing ingestion logic inside Airflow would mean maintaining the same logic in two places. Polling for terminal state, rather than firing the job and assuming success, is what actually enforces the dependency. Without it, "ingestion runs before transformation" is a scheduling accident, not a guarantee.

**Business framing:** Transformations never run against a half-loaded bronze layer, this closes off a whole category of silent, hard-to-diagnose data quality issues downstream.

---

## Credentials live in an Airflow Connection, not in code

**Decision:** Databricks credentials are stored as an Airflow Connection and retrieved at runtime:

```python
conn = BaseHook.get_connection("databricks_default")
```

**Engineering reasoning:** Hardcoded credentials in a DAG file mean the DAG cannot move between environments without editing code, and credentials end up in version control history whether or not anyone intends it to happen. A Connection object is environment-scoped, the same DAG code runs against dev or prod credentials depending on where it's deployed.

**Business framing:** No environment-specific forks of the same pipeline logic, and no credential exposure risk from committed code.

---

# Docker Layout

The dbt project is mounted into the container as a volume. It is not copied into the image at build time.

```text
Host
├── airflow/
└── dbt/

        │
        ▼

Container
/opt/airflow
├── dags
├── plugins
└── dbt
```

**Engineering reasoning:** A mounted volume means SQL, macro, and snapshot changes are visible inside the container the moment they're saved, no rebuild required. The image only needs rebuilding when Python dependencies change, which is a meaningfully rarer event than a model change during active development.

---

## dbt runs in its own virtual environment, isolated from Airflow's

```text
/opt/airflow/dbt_venv
```

Every dbt task calls `/opt/airflow/dbt_venv/bin/dbt` directly, never the Airflow container's default Python.

**Engineering reasoning:** Airflow and dbt are both Python applications with independent, frequently conflicting dependency trees (provider packages vs. adapter packages). Installing dbt into Airflow's environment is asking for a dependency resolution failure the moment either project updates a pinned version. A dedicated venv means each tool's dependencies are resolved independently, in the same container, without fighting each other.

---

# Issues Found and Fixed

Each of these was a real failure during the build, not a hypothetical. Root cause and fix only, no narrative.

**1. Docker volume mount overwrote the dbt virtual environment**
Cause: the venv was created inside the image at the same path where the project volume later got mounted, so the mount silently replaced it at container start.
Symptom: `/opt/airflow/dbt_venv/bin/dbt: No such file or directory`, which reads like a failed install but isn't.
Fix: create the venv at a path outside the mounted project directory so the volume mount can never overwrite it.

**2. `uv` used to install into a venv that didn't contain `uv`**
Cause: `uv venv` creates a Python environment, not a copy of the `uv` binary inside it. Calling `dbt_venv/bin/uv` fails because that binary was never placed there.
Fix: run `uv pip install --python /opt/airflow/dbt_venv/bin/python dbt-core dbt-databricks` from the environment that already has `uv` installed, targeting the venv's Python explicitly, instead of expecting `uv` to exist inside the target venv.

**3. dbt CLI argument order changed in dbt Core 1.11**
Cause: global flags (`--project-dir`, `--profiles-dir`) now must follow the subcommand, not precede it.
Fix: `dbt debug --project-dir ... --profiles-dir ...` instead of `dbt --project-dir ... debug`. Applies to every dbt subcommand, not just `debug`.

**4. Nested dbt project caused dbt to load the wrong `dbt_project.yml`**
Cause: two `dbt_project.yml` files existed at different directory levels in the repo. dbt resolved to the wrong one, producing `No nodes selected` and selector errors like `'silver_tech' does not match any enabled nodes`.
Fix: flattened the repo to a single dbt root with exactly one active `dbt_project.yml`.

**5. Flattening the project broke package resolution**
Cause: after the restructure, the active project no longer had its `dbt_packages` directory populated, producing `dbt found 1 package(s) specified in packages.yml, but only 0 package(s) installed`.
Fix: `dbt deps` reinstalls packages against the current project structure. The DAG now runs `dbt deps` as the first task on every execution, not just after a restructure, so this class of failure can't reoccur silently.

**6. Host paths and container paths are not the same paths**
Cause: the dbt project resolves to different absolute paths on the host machine versus inside the container (`dbt/` vs `/opt/airflow/dbt`).
Fix: the DAG hardcodes container paths only. Orchestration logic never depends on the developer's local filesystem layout, which also means the DAG behaves identically regardless of which machine builds the image.

---

# Known Limitations (Stated, Not Discovered Live)

- No environment split yet (dev/staging/prod). One Airflow Connection, one target. Flagged as an open item, not yet closed.
- No deferred/async operators for Databricks job polling. Current polling holds a worker slot for the duration of the ingestion job, which doesn't scale past a small number of concurrent DAG runs.
- No dbt state-based selective runs. Every execution runs the full DAG regardless of what actually changed upstream.
- No persisted `manifest.json` / `run_results.json` between runs, so there's no artifact-based lineage or historical run comparison yet.
- No failure alerting configured. A failed DAG run is visible in the Airflow UI only, not pushed anywhere.

# Future Improvements

- Replace manual polling with Databricks deferrable operators to free worker slots during ingestion.
- Adopt dbt state comparison (`--select state:modified+`) to run only what changed instead of the full DAG every time.
- Persist dbt artifacts for lineage tracking and historical run comparison.
- Add failure alerting (Slack or email).
- Parameterize environments (dev, staging, production) through Airflow Variables or per-environment configuration, closing the limitation above.
