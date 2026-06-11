"""Explicit Spark schemas for parsing bronze JSON payloads.

Never infer schemas in production jobs: inference is non-deterministic
(one bad batch changes a column type) and hides upstream contract breaks.
These StructTypes mirror ``marketpulse.models`` exactly.
"""

from __future__ import annotations

from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

TICK_SCHEMA = StructType(
    [
        StructField("event_type", StringType()),
        StructField("symbol", StringType()),
        StructField("venue", StringType()),
        StructField("seq", LongType()),
        StructField("event_ts", StringType()),
        StructField("bid", DoubleType()),
        StructField("ask", DoubleType()),
        StructField("bid_size", IntegerType()),
        StructField("ask_size", IntegerType()),
        StructField("last", DoubleType()),
    ]
)

TRADE_SCHEMA = StructType(
    [
        StructField("event_type", StringType()),
        StructField("trade_id", StringType()),
        StructField("symbol", StringType()),
        StructField("venue", StringType()),
        StructField("event_ts", StringType()),
        StructField("price", DoubleType()),
        StructField("qty", IntegerType()),
        StructField("side", StringType()),
        StructField("order_type", StringType()),
        StructField("trader_id", StringType()),
    ]
)
