import logging
import os
import time
from datetime import datetime, timedelta, timezone

from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import RunLifeCycleState, RunResultState

log = logging.getLogger(__name__)

DBT_ROOT = "/opt/airflow/dbt"
DBT_BIN = "/opt/airflow/dbt_venv/bin/dbt"
DBT_PROJECT_DIR = DBT_ROOT
DBT_PROFILES_DIR = DBT_ROOT
DBT_TARGET = "dev"
POLL_INTERVAL = 20

S3_FRESHNESS_WINDOW = timedelta(hours=25)

DBT_ARGS = f"--project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR} --target {DBT_TARGET}"

TERMINAL_STATES = (
    RunLifeCycleState.TERMINATED,
    RunLifeCycleState.SKIPPED,
    RunLifeCycleState.INTERNAL_ERROR,
)

# PURPOSE: this DAG exists only to produce a wall-clock baseline to compare against
# orchestrate_parallel.py. Same tasks, same dbt selectors, fully serial dependency chain.
# Do not schedule this alongside the parallel DAG in production, it's a benchmark artifact.
# Trigger both manually, pull Gantt charts for both runs, compare, delete or keep paused after.


@dag(
    dag_id="orchestrate_sequential_baseline",
    start_date=datetime(2026, 7, 20),
    schedule=None,  # manual trigger only, this is a benchmark, not a production DAG
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "Mohamed Taha Abo Heiba",
        "retries": 0,  # no retries, we want clean timing data, not retry noise
    },
    tags=["Databricks", "dbt", "benchmark"],
)
def orchestrate_sequential_baseline():

    @task
    def ingest():
        conn = BaseHook.get_connection("databricks_default")
        ws = WorkspaceClient(host=conn.host, token=conn.password)

        job_id = int(os.getenv("DATABRICKS_JOB_ID"))
        run = ws.jobs.run_now(job_id=job_id)
        log.info("Triggered Postgres bronze job %s, run id %s", job_id, run.run_id)

        while True:
            job_run = ws.jobs.get_run(run.run_id)
            lifecycle = job_run.state.life_cycle_state
            result = job_run.state.result_state

            if lifecycle in TERMINAL_STATES:
                if result == RunResultState.SUCCESS:
                    log.info("Bronze query-based incremental ingestion completed")
                    return "Bronze ingestion completed"
                raise RuntimeError(f"Postgres bronze job failed: lifecycle={lifecycle}, result={result}")

            time.sleep(POLL_INTERVAL)

    @task
    def check_s3_pipeline_health():
        conn = BaseHook.get_connection("databricks_default")
        ws = WorkspaceClient(host=conn.host, token=conn.password)

        pipeline_id = os.getenv("S3_PIPELINE_ID")
        pipeline = ws.pipelines.get(pipeline_id=pipeline_id)

        if not pipeline.latest_updates:
            raise RuntimeError(f"S3 pipeline {pipeline_id} has no recorded update history at all")

        latest = pipeline.latest_updates[0]
        state_value = latest.state.value
        creation_dt = datetime.fromisoformat(latest.creation_time.replace("Z", "+00:00"))
        age = datetime.now(timezone.utc) - creation_dt

        if age > S3_FRESHNESS_WINDOW:
            raise RuntimeError(
                f"S3 pipeline {pipeline_id} last update is {age} old, "
                f"exceeds {S3_FRESHNESS_WINDOW} freshness window. Last recorded state: {state_value}"
            )

        if state_value != "COMPLETED":
            raise RuntimeError(
                f"S3 pipeline {pipeline_id} latest update state is '{state_value}', expected 'COMPLETED'"
            )

        log.info("S3 pipeline health check passed: state=%s, age=%s", state_value, age)
        return f"S3 pipeline healthy: state={state_value}, age={age}"

    @task.bash
    def debug():
        return f"{DBT_BIN} debug {DBT_ARGS}"

    @task.bash
    def deps():
        return f"{DBT_BIN} deps {DBT_ARGS}"

    @task.bash
    def clean_target():
        return f"rm -rf {DBT_PROJECT_DIR}/target"

    @task.bash
    def source_freshness():
        return f"{DBT_BIN} source freshness {DBT_ARGS}"

    @task.bash
    def silver_technical():
        return f"{DBT_BIN} run {DBT_ARGS} --select silver_tech"

    @task.bash
    def silver_technical_tests():
        return f"{DBT_BIN} test {DBT_ARGS} --select silver_tech"

    @task.bash
    def gold_dim_date():
        return f"{DBT_BIN} run {DBT_ARGS} --select dim_date"

    @task.bash
    def silver_business():
        return f"{DBT_BIN} run {DBT_ARGS} --select silver_business"

    @task.bash
    def silver_business_tests():
        return f"{DBT_BIN} test {DBT_ARGS} --select silver_business"

    @task.bash
    def gold_ephemeral():
        return f"{DBT_BIN} run {DBT_ARGS} --select gold/ephemeral"

    @task.bash
    def gold_snapshot():
        return f"{DBT_BIN} snapshot {DBT_ARGS}"

    @task.bash
    def fact_orders():
        return f"{DBT_BIN} run {DBT_ARGS} --select fact_orders"

    @task.bash
    def gold_dim_supplier():
        return f"{DBT_BIN} run {DBT_ARGS} --select dim_supplier"

    @task.bash
    def fact_supplier_deliveries():
        return f"{DBT_BIN} run {DBT_ARGS} --select fact_supplier_deliveries"

    @task.bash
    def gold_tests():
        return f"{DBT_BIN} test {DBT_ARGS} --select gold"

    (
        ingest()
        >> check_s3_pipeline_health()
        >> debug()
        >> deps()
        >> clean_target()
        >> source_freshness()
        >> silver_technical()
        >> silver_technical_tests()
        >> gold_dim_date()
        >> silver_business()
        >> silver_business_tests()
        >> gold_ephemeral()
        >> gold_snapshot()
        >> fact_orders()
        >> gold_dim_supplier()
        >> fact_supplier_deliveries()
        >> gold_tests()
    )


orchestrate_sequential_baseline_dag = orchestrate_sequential_baseline()