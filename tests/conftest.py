"""Shared fixtures. The Spark session is module-scoped and local —
tests need zero services, which keeps CI fast and contributors happy."""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

# Force local mode regardless of any developer .env
os.environ["MP_ENV"] = "local"
os.environ["MP_DELTA_ENABLED"] = "false"


@pytest.fixture(scope="session")
def spark():
    from marketpulse.utils.spark import build_spark

    spark = build_spark("tests", local=True)
    yield spark
    spark.stop()


@pytest.fixture()
def bronze_events(spark, tmp_path):
    """A small bronze frame built from simulator output, with known dirt."""
    import json

    from pyspark.sql import functions as F

    from marketpulse.generator import MarketSimulator, SimulatorConfig

    sim = MarketSimulator(SimulatorConfig(symbols=["AAPL", "GS"], seed=7, anomaly_rate=0.05))
    events_file = tmp_path / "events.jsonl"
    with events_file.open("w") as fh:
        for event in sim.stream(2000):
            fh.write(json.dumps(event) + "\n")

    raw = spark.read.text(str(events_file)).withColumnRenamed("value", "raw_value")
    return (
        raw.withColumn("kafka_topic", F.lit("test"))
        .withColumn("kafka_partition", F.lit(0))
        .withColumn("kafka_offset", F.monotonically_increasing_id())
        .withColumn("kafka_ts", F.current_timestamp())
        .withColumn("ingested_at", F.current_timestamp())
        .withColumn("ingest_date", F.to_date(F.current_timestamp()))
    )
