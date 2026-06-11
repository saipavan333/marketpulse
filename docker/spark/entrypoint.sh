#!/usr/bin/env bash
# Start a Spark standalone master or worker depending on SPARK_MODE.
set -euo pipefail

SPARK_MODE="${SPARK_MODE:-master}"

if [[ "${SPARK_MODE}" == "master" ]]; then
  echo ">>> Starting Spark MASTER"
  exec /opt/spark/bin/spark-class org.apache.spark.deploy.master.Master \
    --host 0.0.0.0 --port 7077 --webui-port 8080
elif [[ "${SPARK_MODE}" == "worker" ]]; then
  echo ">>> Starting Spark WORKER -> ${SPARK_MASTER_URL}"
  exec /opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker \
    --webui-port 8081 \
    --cores "${SPARK_WORKER_CORES:-2}" \
    --memory "${SPARK_WORKER_MEMORY:-2G}" \
    "${SPARK_MASTER_URL}"
else
  echo "Unknown SPARK_MODE: ${SPARK_MODE}" >&2
  exit 1
fi
