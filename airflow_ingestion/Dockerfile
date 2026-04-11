FROM apache/airflow:2.7.3

USER root
RUN apt-get update \
    && apt-get install -y git build-essential python3-dev \
    && rm -rf /var/lib/apt/lists/*

USER airflow
COPY airflow_ingestion/requirements.txt /opt/airflow/requirements.txt
RUN python3 -m pip install --no-cache-dir --user -r /opt/airflow/requirements.txt

ENV PATH="/home/airflow/.local/bin:${PATH}"
COPY . /opt/airflow/project

ENV AIRFLOW_HOME=/opt/airflow
ENV AIRFLOW__CORE__DAGS_FOLDER=/opt/airflow/project/airflow_ingestion/dags
ENV AIRFLOW__CORE__PLUGINS_FOLDER=/opt/airflow/project/airflow/plugins
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False
WORKDIR /opt/airflow/project
