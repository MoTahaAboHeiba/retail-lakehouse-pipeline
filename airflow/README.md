# Airflow orchestration

This directory contains the orchestration layer for the retail lakehouse pipeline.

Airflow doesn't ingest data and doesn't transform data. It sequences two systems that already do those jobs: a Databricks job for ingestion, and dbt for every transformation. The DAG's only job is ordering, dependency enforcement, and failure isolation.

---

## Pipeline flow

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

### Proof

![Sequential DAG run](../docs/sequential-dag-run.jpg)

The hard dependency chain above, running. Each stage waits on the prior stage's success.

![Parallel DAG run](../docs/parallel-dag-run.jpg)

Tasks with no dependency on each other run in parallel within a stage. The DAG only serializes where a real dependency exists.

---

## Project structure

```text
airflow/
├── dags/
│   └── orchestrate.py
├── config/
├── plugins/
├── .env.example                  # Template — copy to .env and fill in credentials
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
└── README.md
```

---

## Engineering decisions

### Airflow is an orchestrator, not a processing engine

**Decision:** The DAG contains no SQL and no transformation logic. It triggers the Databricks ingestion job, then runs dbt commands in a fixed order.

**Reasoning:** A DAG task that runs raw SQL against the warehouse duplicates work dbt already owns, lineage, dependency resolution, testing, and documentation, without any of that context. Keeping Airflow strictly declarative and dbt strictly transformational means a change to business logic only ever happens in one place.

### `run` and `test` run as separate tasks, not `dbt build`

**Decision:** Every layer runs as two tasks: build, then test. `dbt build` would collapse this into fewer tasks, and I rejected it anyway.

**Reasoning:** `dbt build` reports one pass/fail for a combined operation. Splitting the tasks tells you immediately whether a model failed to build or built but failed validation, two different failure classes with two different fixes.

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

### Ingestion runs as a monitored Databricks job, not inline Airflow logic

**Decision:** Bronze ingestion runs through the Databricks SDK (`WorkspaceClient`) as an external job. Airflow polls the Jobs API until the run reaches a terminal state before any dbt task starts.

**Reasoning:** Ingestion is compute-heavy and belongs on the compute engine, not inside a scheduler container. Polling for terminal state, instead of firing the job and assuming success, is what actually enforces the dependency, without it, "ingestion runs before transformation" is a scheduling accident, not a guarantee.

### Credentials live in an Airflow connection, not in code

**Decision:** Databricks credentials are stored as an Airflow connection and retrieved at runtime:

```python
conn = BaseHook.get_connection("databricks_default")
```

**Reasoning:** Hardcoded credentials in a DAG file mean the DAG can't move between environments without editing code, and they end up in version control history whether or not anyone intends that. A connection object is environment-scoped, so the same DAG code runs against dev or prod credentials depending on where it's deployed.

---

## Docker layout

The dbt project is mounted into the container as a volume, not copied into the image at build time.

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

A mounted volume means SQL, macro, and snapshot changes appear inside the container as soon as you save them, no rebuild required. The image only needs a rebuild when Python dependencies change, which happens far less often than a model change during active development.

### dbt runs in its own virtual environment, isolated from Airflow's

```text
/opt/airflow/dbt_venv
```

Every dbt task calls `/opt/airflow/dbt_venv/bin/dbt` directly, never the Airflow container's default Python.

Airflow and dbt are both Python apps with independent, frequently conflicting dependency trees, provider packages against adapter packages. Installing dbt into Airflow's environment risks a dependency resolution failure the moment either project updates a pinned version. A dedicated venv resolves each tool's dependencies independently, in the same container, without the two fighting each other.

---

## Issues found and fixed

Each of these is a real failure from the build, not a hypothetical. Root cause and fix only, no narrative.

1. **Docker volume mount overwrote the dbt virtual environment.**
   Cause: the venv was created inside the image at the same path where the project volume later got mounted, so the mount silently replaced it at container start.
   Symptom: `/opt/airflow/dbt_venv/bin/dbt: No such file or directory`, which reads like a failed install but isn't.
   Fix: create the venv at a path outside the mounted project directory, so the volume mount can't overwrite it.

2. **`uv` tried to install into a venv that didn't contain `uv`.**
   Cause: `uv venv` creates a Python environment, not a copy of the `uv` binary inside it. Calling `dbt_venv/bin/uv` fails because that binary was never placed there.
   Fix: run `uv pip install --python /opt/airflow/dbt_venv/bin/python dbt-core dbt-databricks` from the environment that already has `uv` installed, targeting the venv's Python directly, instead of expecting `uv` to exist inside the target venv.

3. **The dbt CLI argument order changed in dbt Core 1.11.**
   Cause: global flags (`--project-dir`, `--profiles-dir`) now must follow the subcommand, not precede it.
   Fix: use `dbt debug --project-dir ... --profiles-dir ...`, not `dbt --project-dir ... debug`. This applies to every dbt subcommand, not just `debug`.

4. **A nested dbt project caused dbt to load the wrong `dbt_project.yml`.**
   Cause: two `dbt_project.yml` files existed at different directory levels in the repo. dbt resolved to the wrong one, producing `No nodes selected` and selector errors like `'silver_tech' does not match any enabled nodes`.
   Fix: flatten the repo to a single dbt root with exactly one active `dbt_project.yml`.

5. **Flattening the project broke package resolution.**
   Cause: after the restructure, the active project's `dbt_packages` directory was empty, producing `dbt found 1 package(s) specified in packages.yml, but only 0 package(s) installed`.
   Fix: `dbt deps` reinstalls packages against the current project structure. The DAG now runs `dbt deps` as the first task on every execution, not just after a restructure, so this failure class can't recur silently.

6. **Host paths and container paths don't match.**
   Cause: the dbt project resolves to different absolute paths on the host machine versus inside the container (`dbt/` versus `/opt/airflow/dbt`).
   Fix: the DAG hardcodes container paths only. Orchestration logic never depends on the developer's local filesystem layout, so the DAG behaves identically regardless of which machine builds the image.

---

## Known limitations

- **No environment split yet** (dev/staging/prod). One Airflow connection, one target.
- **No deferred or async operators for Databricks job polling.** Current polling holds a worker slot for the duration of the ingestion job, which doesn't scale past a small number of concurrent DAG runs.
- **No dbt state-based selective runs.** Every execution runs the full DAG regardless of what actually changed upstream.
- **No persisted `manifest.json` or `run_results.json` between runs**, so there's no artifact-based lineage or historical run comparison yet.
- **No failure alerting configured.** A failed DAG run is visible in the Airflow UI only, not pushed anywhere.

## Future improvements

- Replace manual polling with Databricks deferrable operators to free worker slots during ingestion.
- Adopt dbt state comparison (`--select state:modified+`) to run only what changed, instead of the full DAG every time.
- Persist dbt artifacts for lineage tracking and historical run comparison.
- Add failure alerting through Slack or email.
- Parameterize environments (dev, staging, production) through Airflow variables or per-environment configuration.
