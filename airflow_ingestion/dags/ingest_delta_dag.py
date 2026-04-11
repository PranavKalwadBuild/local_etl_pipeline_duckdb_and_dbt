"""Airflow DAG for the ingestion pipeline using PySpark and Delta Lake."""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator

ROOT_DIR = Path(__file__).resolve().parents[2]

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="ingest_delta_pyspark",
    default_args=default_args,
    description="Ingest raw CSV data into Delta Lake using PySpark and load into DuckDB.",
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["ingestion", "pyspark", "delta_lake"],
) as dag:

    ingest_delta = BashOperator(
        task_id="ingest_delta",
        bash_command=f"cd {ROOT_DIR} && python ingest_delta.py",
    )

    load_duckdb = BashOperator(
        task_id="load_delta_to_duckdb",
        bash_command=f"cd {ROOT_DIR} && python delta_to_duckdb.py",
    )

    ingest_delta >> load_duckdb
