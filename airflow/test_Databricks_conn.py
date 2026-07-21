'''
this script is a simple example of how to use the Databricks SDK to trigger a Databricks job from Python.
It uses the WorkspaceClient class to connect to the Databricks workspace and run a job using its job ID. 
Make sure to set the environment variables HOST, TOKEN, and DATABRICKS_JOB_ID before running this script. 
You can set these variables in your operating system or in a .env file
'''
import os
from databricks.sdk import WorkspaceClient


def run_databricks_job():
    ws = WorkspaceClient(
        host=os.getenv("HOST"),
        token=os.getenv("TOKEN")
    )

    job_trigger = ws.jobs.run_now(job_id=os.getenv("DATABRICKS_JOB_ID"))
    return job_trigger

print(run_databricks_job())