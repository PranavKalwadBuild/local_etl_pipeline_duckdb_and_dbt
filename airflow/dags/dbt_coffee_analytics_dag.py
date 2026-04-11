"""Airflow DAG for the coffee_analytics dbt project using DuckDB."""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator

ROOT_DIR = Path(__file__).resolve().parents[2]
DBT_WRAPPER = ROOT_DIR / "run_dbt.py"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="coffee_analytics_dbt",
    default_args=default_args,
    description="Run coffee_analytics dbt jobs using DuckDB.",
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["dbt", "coffee_analytics"],
) as dag:

    dbt_debug = BashOperator(
        task_id="dbt_debug",
        bash_command=f"cd {ROOT_DIR} && python {DBT_WRAPPER} --engine normal -- debug",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {ROOT_DIR} && python {DBT_WRAPPER} --engine normal -- run",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {ROOT_DIR} && python {DBT_WRAPPER} --engine normal -- test",
    )

    dbt_debug >> dbt_run >> dbt_test
