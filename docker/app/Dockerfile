# ============================================================================
# MarketPulse application image
# Contains: marketpulse package, Spark client (PySpark), dbt, Streamlit.
# Used by: `app` (jobs/CLI) and `dashboard` services.
# ============================================================================
# bookworm pinned: trixie-based python:3.10-slim dropped openjdk-17 from apt
FROM python:3.10-slim-bookworm AS base

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

# Java is required by PySpark (Spark driver runs in this container in client mode)
RUN apt-get update && apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless curl git \
    && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

WORKDIR /app

# Install dependencies first (cached layer), then the package
COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install -e ".[spark,delta,kafka,warehouse,dashboard]" \
    && pip install dbt-core==1.8.* dbt-postgres==1.8.*

COPY dbt ./dbt
COPY contracts ./contracts
COPY scripts ./scripts
COPY dashboards ./dashboards

# Pre-fetch Delta + S3A jars into the local ivy cache so first job start is fast
RUN python -c "from marketpulse.utils.spark import build_spark; build_spark('warmup', local=True).stop()" || true

CMD ["python", "-m", "marketpulse.cli", "--help"]
