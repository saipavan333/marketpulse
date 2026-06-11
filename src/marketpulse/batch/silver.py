"""Bronze -> Silver: parse, validate, dedupe, quarantine.

Silver contract:
    - typed columns, event_ts as proper timestamp, trade_date partition
    - **no duplicates** (ticks: symbol+venue+seq, trades: trade_id)
    - **no garbage**: rows violating hard rules are diverted to a
      quarantine table with a reason — never silently dropped, never
      allowed downstream. Auditors (and Goldman interviewers) love this.

All transform functions are pure DataFrame -> DataFrame so they unit-test
without any lake or services.
"""

from __future__ import annotations

import logging

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from marketpulse.batch.schemas import TICK_SCHEMA, TRADE_SCHEMA
from marketpulse.config import get_settings
from marketpulse.utils.spark import build_spark, read_table, write_table

logger = logging.getLogger(__name__)


# Hard validity rules. Rows failing any of these go to quarantine.
# NOTE: rules are factory functions, not module constants — F.col() requires
# an active SparkContext, and module import must never depend on one.
def tick_rules() -> dict[str, F.Column]:
    return {
        "null_symbol": F.col("symbol").isNull(),
        "null_prices": F.col("bid").isNull() | F.col("ask").isNull(),
        "non_positive_price": (F.col("bid") <= 0) | (F.col("ask") <= 0),
        "crossed_quote": F.col("ask") < F.col("bid"),
        "bad_timestamp": F.col("event_ts_parsed").isNull(),
    }


def trade_rules() -> dict[str, F.Column]:
    return {
        "null_trade_id": F.col("trade_id").isNull(),
        "null_price": F.col("price").isNull(),
        "non_positive_price": F.col("price") <= 0,
        "null_qty": F.col("qty").isNull(),
        "non_positive_qty": F.col("qty") <= 0,
        "bad_timestamp": F.col("event_ts_parsed").isNull(),
    }


def parse_bronze(bronze: DataFrame, event_type: str) -> DataFrame:
    """Parse raw JSON strings of one event type into typed columns."""
    schema = TICK_SCHEMA if event_type == "tick" else TRADE_SCHEMA
    return (
        bronze.withColumn("parsed", F.from_json(F.col("raw_value"), schema))
        .where(F.col("parsed.event_type") == event_type)
        .select("parsed.*", "kafka_offset", "ingested_at")
        .withColumn("event_ts_parsed", F.to_timestamp("event_ts"))
    )


def split_quarantine(df: DataFrame, rules: dict[str, F.Column]) -> tuple[DataFrame, DataFrame]:
    """Split a frame into (clean, quarantined-with-reason)."""
    reason = F.lit(None).cast("string")
    for name, predicate in rules.items():
        reason = F.when(predicate & reason.isNull(), F.lit(name)).otherwise(reason)
    tagged = df.withColumn("quarantine_reason", reason)
    clean = tagged.where(F.col("quarantine_reason").isNull()).drop("quarantine_reason")
    bad = tagged.where(F.col("quarantine_reason").isNotNull())
    return clean, bad


def dedupe(df: DataFrame, keys: list[str], order_col: str = "ingested_at") -> DataFrame:
    """Keep the first-ingested row per business key (idempotent replays)."""
    w = Window.partitionBy(*keys).orderBy(F.col(order_col).asc())
    return df.withColumn("_rn", F.row_number().over(w)).where(F.col("_rn") == 1).drop("_rn")


def build_silver_ticks(bronze: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Bronze events -> (silver_ticks, quarantine_ticks)."""
    parsed = parse_bronze(bronze, "tick")
    clean, bad = split_quarantine(parsed, tick_rules())
    silver = (
        dedupe(clean, keys=["symbol", "venue", "seq"])
        .withColumn("mid", (F.col("bid") + F.col("ask")) / 2)
        .withColumn("spread_bps", (F.col("ask") - F.col("bid")) / F.col("mid") * 10_000)
        .withColumn("event_ts", F.col("event_ts_parsed"))
        .withColumn("trade_date", F.to_date("event_ts_parsed"))
        .drop("event_ts_parsed")
    )
    return silver, bad


def build_silver_trades(bronze: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Bronze events -> (silver_trades, quarantine_trades)."""
    parsed = parse_bronze(bronze, "trade")
    clean, bad = split_quarantine(parsed, trade_rules())
    silver = (
        dedupe(clean, keys=["trade_id"])
        .withColumn("notional", F.col("price") * F.col("qty"))
        .withColumn("event_ts", F.col("event_ts_parsed"))
        .withColumn("trade_date", F.to_date("event_ts_parsed"))
        .drop("event_ts_parsed")
    )
    return silver, bad


def run(overwrite: bool = True) -> dict[str, int]:
    """Job entrypoint: read bronze, write silver + quarantine tables."""
    settings = get_settings()
    spark = build_spark("silver")
    bronze = read_table(spark, "bronze", "events")

    counts: dict[str, int] = {"bronze": bronze.count()}
    mode = "overwrite" if overwrite else "append"

    for name, builder in (("ticks", build_silver_ticks), ("trades", build_silver_trades)):
        silver, bad = builder(bronze)
        write_table(silver, "silver", name, mode=mode, partition_by=["trade_date"])
        write_table(bad, "silver", f"quarantine_{name}", mode=mode)
        counts[f"silver_{name}"] = silver.count()
        counts[f"quarantine_{name}"] = bad.count()
        logger.info(
            "silver_%s: %s clean / %s quarantined",
            name,
            counts[f"silver_{name}"],
            counts[f"quarantine_{name}"],
        )
    logger.info("Silver complete: %s (env=%s)", counts, settings.env)
    return counts


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
