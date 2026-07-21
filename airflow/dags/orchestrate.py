import logging
import os
import time
from datetime import datetime, timedelta

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

DBT_ARGS = f"--project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR} --target {DBT_TARGET}"

TERMINAL_STATES = (
    RunLifeCycleState.TERMINATED,
    RunLifeCycleState.SKIPPED,
    RunLifeCycleState.INTERNAL_ERROR,
)


@dag(
    dag_id="orchestrate",
    start_date=datetime(2026, 7, 20),
    schedule="0 2 * * *",
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "Mohamed Taha Abo Heiba",
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
    },
    tags=["Databricks", "dbt"],
)
def orchestrate():

    @task
    def ingest():
        conn = BaseHook.get_connection("databricks_default")
        ws = WorkspaceClient(host=conn.host, token=conn.password)

        job_id = int(os.getenv("DATABRICKS_JOB_ID"))
        run = ws.jobs.run_now(job_id=job_id)
        log.info("Triggered Databricks job %s, run id %s", job_id, run.run_id)

        while True:
            job_run = ws.jobs.get_run(run.run_id)
            lifecycle = job_run.state.life_cycle_state
            result = job_run.state.result_state

            if lifecycle in TERMINAL_STATES:
                if result == RunResultState.SUCCESS:
                    log.info("CDC ingestion completed")
                    return "CDC ingestion completed"
                raise RuntimeError(f"Databricks job failed: lifecycle={lifecycle}, result={result}")

            time.sleep(POLL_INTERVAL)

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
    def gold_facts():
        return f"{DBT_BIN} run {DBT_ARGS} --select gold/fact"

    (
        ingest()
        >> debug()
        >> deps()
        >> clean_target()
        >> source_freshness()
        >> silver_technical()
        >> silver_technical_tests()
        >> silver_business()
        >> silver_business_tests()
        >> gold_ephemeral()
        >> gold_snapshot()
        >> gold_facts()
    )


orchestrate_dag = orchestrate()