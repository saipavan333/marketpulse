# ============================================================================
# MarketPulse Airflow image
# Base: official Airflow. Adds Java + PySpark (client mode driver),
# Delta support, dbt, and the marketpulse package (mounted at /opt/airflow/src).
# ============================================================================
FROM apache/airflow:2.10.3-python3.10

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow
RUN pip install --no-cache-dir \
        pyspark==3.5.1 \
        delta-spark==3.2.0 \
        pydantic>=2.5 \
        pydantic-settings>=2.1 \
        pyyaml \
        pandas \
        sqlalchemy \
        psycopg2-binary \
        dbt-core==1.8.* \
        dbt-postgres==1.8.*

# marketpulse source is volume-mounted to /opt/airflow/src by docker-compose
ENV PYTHONPATH=/opt/airflow/src
