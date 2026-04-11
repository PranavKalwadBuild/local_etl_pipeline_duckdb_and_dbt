# Airflow + dbt

This folder contains a basic Airflow DAG for the `coffee_analytics` dbt project.
The DAG runs dbt using the repository wrapper at `run_dbt.py` and the DuckDB targets in `coffee_analytics/profiles.yml`.

A separate ingestion Airflow setup for PySpark lives in `airflow_ingestion/`.

## What is included

- `airflow/dags/dbt_coffee_analytics_dag.py`
  - `dbt_debug`
  - `dbt_run`
  - `dbt_test`

## Requirements

Install the Python dependencies from the project `requirements.txt`:

```bash
pip install -r requirements.txt
```

> If you already have a Python virtual environment, activate it first.

## Docker Compose mode

A ready-to-use Docker Compose setup is provided in `airflow/docker-compose.yml`.
This uses a local Airflow image built from `airflow/Dockerfile`, mounts the repository into the container, and exposes the host Docker socket so Airflow can launch dbt in a separate dbt container.

> Run the compose command from the repository root so `HOST_PROJECT_DIR` points to the correct host path.

Build and start Airflow:

```bash
cd /Users/pranavkalwad/Desktop/Pranav/Career/Modern-data-platform
docker compose -f airflow/docker-compose.yml up airflow-init
```

Start the webserver and scheduler:

```bash
docker compose -f airflow/docker-compose.yml up -d webserver scheduler
```

Open the UI at `http://localhost:8080` and trigger the `coffee_analytics_dbt` DAG.

If you change code, restart the services:

```bash
docker compose -f airflow/docker-compose.yml down
docker compose -f airflow/docker-compose.yml up -d webserver scheduler
```

## Kubernetes mode

Kubernetes manifests are available under `airflow/k8s/`.
The flow uses a locally built Docker image named `local/modern-data-platform-airflow:latest`.

Build the image from the repo root:

```bash
cd /Users/pranavkalwad/Desktop/Pranav/Career/Modern-data-platform
docker build -t local/modern-data-platform-airflow:latest -f airflow/Dockerfile .
```

If you are using a local cluster such as `kind` or `minikube`, also load the image into the cluster:

```bash
kind load docker-image local/modern-data-platform-airflow:latest
# or for minikube:
# minikube image load local/modern-data-platform-airflow:latest
```

## Testing without Airflow

You can validate the dbt flow directly from the repository root:

```bash
python run_dbt.py --engine normal debug
python run_dbt.py --engine normal run
python run_dbt.py --engine normal test
```

This helps confirm the dbt DuckDB targets work before you execute the DAG.
