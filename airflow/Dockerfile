FROM apache/airflow:3.3.0

USER root

RUN apt-get update && apt-get install -y --no-install-recommends gcc && \
    rm -rf /var/lib/apt/lists/*

USER airflow

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

RUN pip install --no-cache-dir uv

RUN uv venv /opt/airflow/dbt_venv

RUN uv pip install \
    --python /opt/airflow/dbt_venv/bin/python \
    dbt-core==1.11.12 \
    dbt-databricks==1.12.2
