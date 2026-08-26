import logging
import os
import smtplib
import time
from datetime import datetime, timedelta, timezone
from email.mime.text import MIMEText

import requests
from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.models import Variable
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

# Failure alerting
#
# Config is read from Airflow Variables/Connections at call time, inside the
# callback. Top-level Variable.get() calls run on
# every DAG file parse (scheduler heartbeat), not just at execution, and add
# unnecessary metadata DB load. Resolving config lazily avoids that.
#
# Required Airflow Variables:
#   telegram_bot_token   - Telegram bot token from BotFather
#   telegram_chat_id     - target chat id (user, group, or channel)
#   alert_email_to       - recipient email address for failure alerts
#
# Required Airflow Connection:
#   gmail_smtp           - conn_type=smtp/generic, login=<gmail address>,
#                           password=<Gmail App Password, not account password>,
#                           host=smtp.gmail.com (optional, defaults below),
#                           port=465 (optional, defaults below)


def _send_telegram(message: str) -> None:
    bot_token = Variable.get("telegram_bot_token", default_var=None)
    chat_id = Variable.get("telegram_chat_id", default_var=None)

    if not bot_token or not chat_id:
        log.error("Telegram alert skipped: telegram_bot_token or telegram_chat_id not set")
        return

    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    try:
        resp = requests.post(
            url,
            json={"chat_id": chat_id, "text": message, "parse_mode": "HTML"},
            timeout=10,
        )
        resp.raise_for_status()
        log.info("Telegram alert sent")
    except Exception as e:
        log.error("Telegram alert failed to send: %s", e)


def _send_gmail(subject: str, body: str) -> None:
    recipient = Variable.get("alert_email_to", default_var=None)
    if not recipient:
        log.error("Email alert skipped: alert_email_to not set")
        return

    try:
        conn = BaseHook.get_connection("gmail_smtp")
    except Exception as e:
        log.error("Email alert skipped: gmail_smtp connection not found (%s)", e)
        return

    msg = MIMEText(body)
    msg["Subject"] = subject
    msg["From"] = conn.login
    msg["To"] = recipient

    host = conn.host or "smtp.gmail.com"
    port = conn.port or 465

    try:
        with smtplib.SMTP_SSL(host, port, timeout=15) as server:
            server.login(conn.login, conn.password)
            server.sendmail(conn.login, [recipient], msg.as_string())
        log.info("Email alert sent to %s", recipient)
    except Exception as e:
        log.error("Email alert failed to send: %s", e)


def notify_failure(context: dict) -> None:
    """on_failure_callback. Fires once per task instance, after retries are
    exhausted (Airflow moves a task to up_for_retry, not failed, between
    attempts). Sends the same alert to Gmail and Telegram. 
    """
    ti = context.get("task_instance")
    exception = context.get("exception")
    logical_date = context.get("logical_date") or context.get("execution_date")

    dag_id = ti.dag_id if ti else context.get("dag").dag_id
    task_id = ti.task_id if ti else "unknown"
    try_number = ti.try_number if ti else "unknown"
    log_url = ti.log_url if ti else "unavailable"

    subject = f"[Airflow] {dag_id}.{task_id} FAILED"
    body = (
        f"DAG: {dag_id}\n"
        f"Task: {task_id}\n"
        f"Try: {try_number}\n"
        f"Logical date: {logical_date}\n"
        f"Exception: {exception}\n"
        f"Log: {log_url}\n"
    )

    _send_gmail(subject, body)
    _send_telegram(f"<b>{subject}</b>\n{body}")


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
        "on_failure_callback": notify_failure,
    },
    tags=["Databricks", "dbt", "parallel"],
)
def orchestrate_parallel():

    @task(execution_timeout=timedelta(minutes=45))
    def ingest():
        """Triggers the Postgres bronze job via incremental connector.
        execution_timeout=45min is a placeholder. Replace with measured p95 from
        ws.jobs.list_runs(job_id=...) once you have enough run history.
        """
        conn = BaseHook.get_connection("databricks_default")
        ws = WorkspaceClient(host=conn.host, token=conn.password)

        job_id = int(os.getenv("DATABRICKS_JOB_ID"))
        run = ws.jobs.run_now(job_id=job_id)
        run_id = run.run_id
        log.info("Triggered Postgres bronze job %s, run id %s", job_id, run_id)

        try:
            while True:
                job_run = ws.jobs.get_run(run_id)
                lifecycle = job_run.state.life_cycle_state
                result = job_run.state.result_state

                if lifecycle in TERMINAL_STATES:
                    if result == RunResultState.SUCCESS:
                        log.info("Bronze query-based incremental ingestion completed")
                        return "Bronze ingestion completed"
                    raise RuntimeError(f"Postgres bronze job failed: lifecycle={lifecycle}, result={result}")

                time.sleep(POLL_INTERVAL)
        except Exception:
            log.error("ingest() exiting abnormally, cancelling Databricks run %s", run_id)
            try:
                ws.jobs.cancel_run(run_id=run_id)
            except Exception as cancel_err:
                log.error("Failed to cancel Databricks run %s: %s", run_id, cancel_err)
            raise

    @task(execution_timeout=timedelta(minutes=5))
    def check_s3_pipeline_health():
        """Confirms the S3 Lakeflow Connect pipeline actually ran and succeeded today.
        This does NOT trigger the pipeline, it runs on its own native Databricks schedule.
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

    #  Sales branch (independent from here) 

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

    # Procurement branch (independent from here) 

    @task.bash
    def gold_dim_supplier():
        return f"{DBT_BIN} run {DBT_ARGS} --select dim_supplier"

    @task.bash
    def fact_supplier_deliveries():
        return f"{DBT_BIN} run {DBT_ARGS} --select fact_supplier_deliveries"

    # Join point 

    @task.bash
    def gold_tests():
        # Runs after BOTH branches complete
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
