FROM python:3.11-slim

WORKDIR /opt/app

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends git ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY coffee_analytics/requirements.txt /opt/app/requirements.txt
RUN python -m pip install --upgrade pip setuptools wheel \
    && python -m pip install --no-cache-dir -r /opt/app/requirements.txt

COPY coffee_analytics /opt/app/coffee_analytics

ENV PATH="/root/.local/bin:${PATH}"
WORKDIR /opt/app/coffee_analytics
ENTRYPOINT ["dbt"]
CMD ["--help"]
