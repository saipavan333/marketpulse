"""Spark Structured Streaming: Kafka -> bronze layer.

Bronze philosophy (interview talking point):
    Bronze is an **immutable audit log**. We keep the raw JSON string
    exactly as it arrived plus ingestion metadata (topic/partition/offset/
    timestamp). No parsing failures can lose data: unparseable payloads
    still land in bronze and are quarantined later, never dropped silently.

Exactly-once-ish delivery:
    Kafka source offsets + Spark checkpointing + idempotent sinks
    (Delta transactional writes) give end-to-end at-least-once with
    deterministic replays; silver's dedupe makes it effectively-once.
"""

from __future__ import annotations

import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from marketpulse.config import get_settings
from marketpulse.utils.spark import build_spark

logger = logging.getLogger(__name__)

BRONZE_COLUMNS = [
    "raw_value",
    "kafka_topic",
    "kafka_partition",
    "kafka_offset",
    "kafka_ts",
    "ingested_at",
    "ingest_date",
]


def with_bronze_metadata(kafka_df: DataFrame) -> DataFrame:
    """Project a raw Kafka frame into the bronze envelope schema."""
    return kafka_df.select(
        F.col("value").cast("string").alias("raw_value"),
        F.col("topic").alias("kafka_topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_ts"),
        F.current_timestamp().alias("ingested_at"),
        F.to_date(F.current_timestamp()).alias("ingest_date"),
    )


def start_bronze_stream(
    spark: SparkSession | None = None,
    topics: str | None = None,
    await_termination: bool = True,
):
    """Start the Kafka -> bronze streaming query (one query, both topics)."""
    settings = get_settings()
    spark = spark or build_spark("bronze-stream")
    topics = topics or f"{settings.topic_ticks},{settings.topic_trades}"

    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap)
        .option("subscribe", topics)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", 50_000)  # backpressure: bounded micro-batches
        .option("failOnDataLoss", "false")
        .load()
    )

    bronze = with_bronze_metadata(kafka_df)

    query = (
        bronze.writeStream.format(settings.table_format)
        .outputMode("append")
        .option("checkpointLocation", f"{settings.checkpoint_root}/bronze_events")
        .partitionBy("ingest_date", "kafka_topic")
        .trigger(processingTime="30 seconds")
        .start(settings.lake_path("bronze", "events"))
    )
    logger.info("Bronze stream started -> %s", settings.lake_path("bronze", "events"))
    if await_termination:
        query.awaitTermination()
    return query
