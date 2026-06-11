"""Spark session factory + format-agnostic table IO.

One builder for every job means S3/MinIO credentials, Delta extensions and
shuffle tuning are configured in exactly one place.

``read_table`` / ``write_table`` abstract the storage format:
    - local mode  -> Parquet on the local filesystem (zero-dependency tests)
    - docker/prod -> Delta Lake on MinIO/S3 (ACID, time travel, compaction)

The business logic in ``batch/`` is identical in both modes — that is the
point: transforms should not care where bytes live (storage/compute split).
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from marketpulse.config import Settings, get_settings

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)


def build_spark(app_name: str, local: bool | None = None, settings: Settings | None = None):
    """Create (or fetch) a configured SparkSession."""
    from pyspark.sql import SparkSession

    settings = settings or get_settings()
    is_local = local if local is not None else settings.env == "local"

    builder = (
        SparkSession.builder.appName(f"marketpulse-{app_name}")
        .config("spark.sql.shuffle.partitions", str(settings.spark_shuffle_partitions))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.adaptive.enabled", "true")
    )
    builder = builder.master("local[*]") if is_local else builder.master(settings.spark_master)

    if settings.delta_enabled:
        builder = builder.config(
            "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
        ).config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )

    if settings.lake_root.startswith("s3a://"):
        builder = (
            builder.config("spark.hadoop.fs.s3a.endpoint", settings.s3_endpoint)
            .config("spark.hadoop.fs.s3a.access.key", settings.s3_access_key)
            .config("spark.hadoop.fs.s3a.secret.key", settings.s3_secret_key)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config(
                "spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
            )
        )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def write_table(
    df: DataFrame,
    layer: str,
    table: str,
    mode: str = "append",
    partition_by: list[str] | None = None,
    settings: Settings | None = None,
) -> str:
    """Write a DataFrame to the lakehouse; returns the table path."""
    settings = settings or get_settings()
    path = settings.lake_path(layer, table)
    writer = df.write.format(settings.table_format).mode(mode)
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.save(path)
    logger.info("Wrote %s rows=? -> %s (%s)", table, path, settings.table_format)
    return path


def read_table(spark: SparkSession, layer: str, table: str, settings: Settings | None = None):
    """Read a lakehouse table as a DataFrame."""
    settings = settings or get_settings()
    path = settings.lake_path(layer, table)
    return spark.read.format(settings.table_format).load(path)
