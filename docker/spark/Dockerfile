# ============================================================================
# MarketPulse Spark image (master + worker, selected via SPARK_MODE env)
# Base: official Apache Spark 3.5.1 with Python bindings.
# Adds: Delta Lake + S3A (hadoop-aws) jars so executors can read/write
#       Delta tables on MinIO/S3.
# ============================================================================
FROM apache/spark:3.5.1-python3

USER root

# --- Delta Lake + S3A jars (versions matched to Spark 3.5.1 / Hadoop 3.3.4) ---
ARG DELTA_VERSION=3.2.0
ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_SDK_VERSION=1.12.262
ARG MAVEN=https://repo1.maven.org/maven2

RUN curl -fsSL -o /opt/spark/jars/delta-spark_2.12-${DELTA_VERSION}.jar \
        ${MAVEN}/io/delta/delta-spark_2.12/${DELTA_VERSION}/delta-spark_2.12-${DELTA_VERSION}.jar \
 && curl -fsSL -o /opt/spark/jars/delta-storage-${DELTA_VERSION}.jar \
        ${MAVEN}/io/delta/delta-storage/${DELTA_VERSION}/delta-storage-${DELTA_VERSION}.jar \
 && curl -fsSL -o /opt/spark/jars/hadoop-aws-${HADOOP_AWS_VERSION}.jar \
        ${MAVEN}/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar \
 && curl -fsSL -o /opt/spark/jars/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar \
        ${MAVEN}/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar

# Python deps used inside executors (kept minimal on purpose)
RUN pip install --no-cache-dir pydantic pydantic-settings pyyaml

COPY docker/spark/entrypoint.sh /opt/entrypoint-mp.sh
RUN chmod +x /opt/entrypoint-mp.sh

USER spark
ENTRYPOINT ["/opt/entrypoint-mp.sh"]
