FROM python:3.11-slim

WORKDIR /app

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends git ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /app/requirements.txt
RUN python -m pip install --upgrade pip setuptools wheel \
    && python -m pip install --no-cache-dir -r /app/requirements.txt

# Default: interactive query shell. Override with "python load.py" to ingest data.
CMD ["python", "query_duckdb.py"]
