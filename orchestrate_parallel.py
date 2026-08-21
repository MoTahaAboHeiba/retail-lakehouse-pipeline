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

# S3 pipeline is now on a native daily schedule inside Databricks (see decision log),
# timed to complete before this DAG's 2am run. This is the max age we accept before
# treating its last update as stale rather than "just hasn't run yet today."
S3_FRESHNESS_WINDOW = timedelta(hours=25)

DBT_ARGS = f"--project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR} --target {DBT_TARGET}"

TERMINAL_STATES = (
    RunLifeCycleState.TERMINATED,
    RunLifeCycleState.SKIPPED,
    RunLifeCycleState.INTERNAL_ERROR,
)


@dag(
    dag_id="orchestrate_parallel",
    start_date=datetime(2026, 7, 20),
    schedule="0 2 * * *",
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "Mohamed Taha Abo Heiba",
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
    },
    tags=["Databricks", "dbt", "parallel"],
)
def orchestrate_parallel():

    @task
    def ingest():
        """Triggers the Postgres bronze job via query-based incremental connector.
        This is NOT CDC (Decision #5). Do not rename this back."""
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
        """Confirms the S3 Lakeflow Connect pipeline actually ran and succeeded today.
        This does NOT trigger the pipeline, it runs on its own native Databricks schedule.
        This is the failure-visibility check Airflow was missing entirely before today.

        VERIFY BEFORE TRUSTING: field names below (latest_updates, .state, .creation_time)
        are from SDK memory, not confirmed against your installed databricks-sdk version.
        Run a throwaway script that does:
            print(ws.pipelines.get(pipeline_id=<id>))
        and confirm these fields exist and mean what this code assumes before relying on it.
        """
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
        # Confirm error_after/warn_after are actually configured in sources.yml for
        # BOTH the Postgres and S3 bronze sources. Without thresholds this task passes
        # trivially and gives you nothing.
        return f"{DBT_BIN} source freshness {DBT_ARGS}"

    @task.bash
    def silver_technical():
        return f"{DBT_BIN} run {DBT_ARGS} --select silver_tech"

    @task.bash
    def silver_technical_tests():
        return f"{DBT_BIN} test {DBT_ARGS} --select silver_tech"

    @task.bash
    def gold_dim_date():
        # Shared conformed dimension, needs both orders_tech and supplier_deliveries_tech.
        # This is the sync point both branches depend on before they can fork.
        return f"{DBT_BIN} run {DBT_ARGS} --select dim_date"

    # ---- Sales branch (independent from here) ----

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
        # Model-name select, not folder select. Folder select would pull in
        # fact_supplier_deliveries too and defeat the whole point of splitting this.
        return f"{DBT_BIN} run {DBT_ARGS} --select fact_orders"

    # ---- Procurement branch (independent from here) ----

    @task.bash
    def gold_dim_supplier():
        return f"{DBT_BIN} run {DBT_ARGS} --select dim_supplier"

    @task.bash
    def fact_supplier_deliveries():
        return f"{DBT_BIN} run {DBT_ARGS} --select fact_supplier_deliveries"

    # ---- Join point ----

    @task.bash
    def gold_tests():
        # Runs after BOTH branches complete. This is the test coverage that was
        # missing entirely before today, gold layer shipped with zero automated
        # test enforcement in the DAG.
        return f"{DBT_BIN} test {DBT_ARGS} --select gold"

    ingest_t = ingest()
    s3_health_t = check_s3_pipeline_health()
    debug_t = debug()
    deps_t = deps()
    clean_t = clean_target()
    freshness_t = source_freshness()
    silver_tech_t = silver_technical()
    silver_tech_tests_t = silver_technical_tests()
    dim_date_t = gold_dim_date()

    silver_biz_t = silver_business()
    silver_biz_tests_t = silver_business_tests()
    gold_eph_t = gold_ephemeral()
    gold_snap_t = gold_snapshot()
    fact_orders_t = fact_orders()

    dim_supplier_t = gold_dim_supplier()
    fact_supplier_t = fact_supplier_deliveries()

    gold_tests_t = gold_tests()

    # Both ingestion checks must clear before anything dbt-related starts
    [ingest_t, s3_health_t] >> debug_t >> deps_t >> clean_t >> freshness_t
    freshness_t >> silver_tech_t >> silver_tech_tests_t >> dim_date_t

    # Fork: Sales branch
    dim_date_t >> silver_biz_t >> silver_biz_tests_t >> gold_eph_t >> gold_snap_t >> fact_orders_t

    # Fork: Procurement branch, runs concurrently with the Sales branch above
    dim_date_t >> dim_supplier_t >> fact_supplier_t

    # Join: both branches must complete before gold-wide tests run
    [fact_orders_t, fact_supplier_t] >> gold_tests_t


orchestrate_parallel_dag = orchestrate_parallel()