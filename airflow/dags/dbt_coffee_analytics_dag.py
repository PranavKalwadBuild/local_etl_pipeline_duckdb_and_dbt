"""Airflow DAG for the coffee_analytics dbt project using DuckDB."""

from __future__ import annotations

import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator

HOST_PROJECT_DIR = os.environ.get("HOST_PROJECT_DIR", "/opt/airflow/project")
DBT_IMAGE = os.environ.get("DBT_IMAGE", "coffee_analytics_dbt:latest")
DBT_PROJECT_DIR = "/opt/app/coffee_analytics"

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
    description="Run coffee_analytics dbt jobs using DuckDB in a container.",
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["dbt", "coffee_analytics"],
) as dag:

    dbt_debug = DockerOperator(
        task_id="dbt_debug",
        image=DBT_IMAGE,
        api_version="auto",
        docker_url="unix://var/run/docker.sock",
        command=(
            f"debug --profiles-dir {DBT_PROJECT_DIR} "
            f"--project-dir {DBT_PROJECT_DIR}"
        ),
        network_mode="bridge",
        auto_remove=True,
        volumes=[f"{HOST_PROJECT_DIR}:/opt/app:rw"],
        working_dir=DBT_PROJECT_DIR,
        tty=True,
    )

    dbt_run = DockerOperator(
        task_id="dbt_run",
        image=DBT_IMAGE,
        api_version="auto",
        docker_url="unix://var/run/docker.sock",
        command=(
            f"run --profiles-dir {DBT_PROJECT_DIR} "
            f"--project-dir {DBT_PROJECT_DIR} --target dev"
        ),
        network_mode="bridge",
        auto_remove=True,
        volumes=[f"{HOST_PROJECT_DIR}:/opt/app:rw"],
        working_dir=DBT_PROJECT_DIR,
        tty=True,
    )

    dbt_test = DockerOperator(
        task_id="dbt_test",
        image=DBT_IMAGE,
        api_version="auto",
        docker_url="unix://var/run/docker.sock",
        command=(
            f"test --profiles-dir {DBT_PROJECT_DIR} " f"--project-dir {DBT_PROJECT_DIR}"
        ),
        network_mode="bridge",
        auto_remove=True,
        volumes=[f"{HOST_PROJECT_DIR}:/opt/app:rw"],
        working_dir=DBT_PROJECT_DIR,
        tty=True,
    )

    dbt_debug >> dbt_run >> dbt_test
