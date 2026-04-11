# Airflow Ingestion Setup (PySpark + Delta Lake)

This folder contains a separate Airflow setup for the ingestion pipeline.
It runs the repository ingestion scripts using PySpark and Delta Lake:

- `ingest_delta.py`
- `delta_to_duckdb.py`

## What is included

- `airflow_ingestion/dags/ingest_delta_dag.py`
  - `ingest_delta`
  - `load_delta_to_duckdb`

## Requirements

Install the Python dependencies from the ingestion-specific requirements file:

```bash
pip install -r airflow_ingestion/requirements.txt
```

> If you already have a Python virtual environment, activate it first.

## Docker Compose mode

A ready-to-use Docker Compose setup is provided in `airflow_ingestion/docker-compose.yml`.
This uses a local Airflow image built from `airflow_ingestion/Dockerfile` and mounts the repository into the container.

Build and start the Airflow init container:

```bash
cd /Users/pranavkalwad/Desktop/Pranav/Career/Modern-data-platform
docker compose -f airflow_ingestion/docker-compose.yml up airflow-init
```

Start the webserver and scheduler:

```bash
docker compose -f airflow_ingestion/docker-compose.yml up -d webserver scheduler
```

Open the UI at `http://localhost:8080` and trigger `ingest_delta_pyspark`.

If you change code, restart the services:

```bash
docker compose -f airflow_ingestion/docker-compose.yml down
docker compose -f airflow_ingestion/docker-compose.yml up -d webserver scheduler
```

## Kubernetes mode

Kubernetes manifests are available under `airflow_ingestion/k8s/`.
The flow uses a locally built Docker image named `local/modern-data-platform-airflow-ingestion:latest`.

Build the image from the repo root:

```bash
cd /Users/pranavkalwad/Desktop/Pranav/Career/Modern-data-platform
docker build -t local/modern-data-platform-airflow-ingestion:latest -f airflow_ingestion/Dockerfile .
```

If you are using a local cluster such as `kind` or `minikube`, also load the image into the cluster:

```bash
kind load docker-image local/modern-data-platform-airflow-ingestion:latest
# or for minikube:
# minikube image load local/modern-data-platform-airflow-ingestion:latest
```

Apply the Kubernetes resources:

```bash
kubectl apply -f airflow_ingestion/k8s/namespace.yaml
kubectl apply -f airflow_ingestion/k8s/postgres.yaml
kubectl apply -f airflow_ingestion/k8s/airflow-services.yaml
kubectl apply -f airflow_ingestion/k8s/airflow-deployments.yaml
kubectl apply -f airflow_ingestion/k8s/airflow-init-job.yaml
```

Watch the pods until they are ready:

```bash
kubectl get pods -n airflow-ingestion
```

Port-forward the webserver to localhost:

```bash
kubectl port-forward svc/airflow-ingestion-webserver 8080:8080 -n airflow-ingestion
```

Open the Airflow UI at `http://localhost:8080` and trigger `ingest_delta_pyspark`.

## Testing without Airflow

Run the ingestion steps directly from the repository root:

```bash
python3 ingest_delta.py
python3 delta_to_duckdb.py
```

This confirms that the ingestion pipeline works before it is executed by Airflow.

## Notes

- The DAG expects the project root layout to remain the same.
- This setup is intentionally separate from the dbt Airflow workflow in `airflow/`.
