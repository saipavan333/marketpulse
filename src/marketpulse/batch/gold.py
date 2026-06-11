"""Silver -> Gold: business-level aggregates.

Gold tables (consumed by dbt marts, dashboard, and downstream quants):
    gold/ohlcv_1m         per-symbol 1-minute OHLCV bars + VWAP + spread
    gold/symbol_risk_daily realised volatility, VaR(95), drawdown, volumes

Financial definitions used (interview talking points):
    - VWAP: sum(price*qty)/sum(qty) over the window.
    - Realised volatility: stddev of 1-minute log returns, annualised by
      sqrt(252 * 390) (390 trading minutes per session).
    - Historical VaR(95): the 5th percentile of minute log returns —
      "the loss you exceed only 5% of the time", scaled to notional later.
    - Max drawdown: largest peak-to-trough close decline within the day.
"""

from __future__ import annotations

import logging
import math

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from marketpulse.utils.spark import build_spark, read_table, write_table

logger = logging.getLogger(__name__)

ANNUALISATION = math.sqrt(252 * 390)  # minute bars -> annualised vol


def build_ohlcv_1m(silver_ticks: DataFrame, silver_trades: DataFrame) -> DataFrame:
    """1-minute bars per symbol: OHLC from ticks, volume/VWAP from trades."""
    ticks = silver_ticks.withColumn("minute", F.date_trunc("minute", "event_ts"))
    trades = silver_trades.withColumn("minute", F.date_trunc("minute", "event_ts"))

    w_minute = Window.partitionBy("symbol", "minute").orderBy("event_ts")
    w_full = Window.partitionBy("symbol", "minute")

    bars = (
        ticks.withColumn("open", F.first("mid").over(w_minute))
        .withColumn("close", F.last("mid").over(w_full))
        .groupBy("symbol", "minute")
        .agg(
            F.first("open").alias("open"),
            F.max("mid").alias("high"),
            F.min("mid").alias("low"),
            F.first("close").alias("close"),
            F.avg("spread_bps").alias("avg_spread_bps"),
            F.count("*").alias("tick_count"),
        )
    )

    volume = trades.groupBy("symbol", "minute").agg(
        F.sum("qty").alias("volume"),
        F.sum("notional").alias("notional"),
        (F.sum("notional") / F.sum("qty")).alias("vwap"),
        F.count("*").alias("trade_count"),
    )

    return (
        bars.join(volume, ["symbol", "minute"], "left")
        .fillna({"volume": 0, "notional": 0.0, "trade_count": 0})
        .withColumn("trade_date", F.to_date("minute"))
    )


def build_symbol_risk_daily(ohlcv_1m: DataFrame) -> DataFrame:
    """Per-symbol, per-day risk metrics from minute bars."""
    w_sym = Window.partitionBy("symbol", "trade_date").orderBy("minute")
    w_run = w_sym.rowsBetween(Window.unboundedPreceding, Window.currentRow)

    with_returns = (
        ohlcv_1m.withColumn("prev_close", F.lag("close").over(w_sym))
        .withColumn(
            "log_ret",
            F.when(
                F.col("prev_close").isNotNull() & (F.col("prev_close") > 0),
                F.log(F.col("close") / F.col("prev_close")),
            ),
        )
        .withColumn("running_peak", F.max("close").over(w_run))
        .withColumn("drawdown", F.col("close") / F.col("running_peak") - 1)
    )

    return (
        with_returns.groupBy("symbol", "trade_date")
        .agg(
            F.first("open").alias("day_open"),
            F.max("high").alias("day_high"),
            F.min("low").alias("day_low"),
            F.last("close").alias("day_close"),
            F.sum("volume").alias("total_volume"),
            F.sum("notional").alias("total_notional"),
            F.sum("trade_count").alias("trade_count"),
            (F.stddev("log_ret") * F.lit(ANNUALISATION)).alias("realised_vol_annualised"),
            F.expr("percentile_approx(log_ret, 0.05)").alias("var_95_log_ret"),
            F.min("drawdown").alias("max_drawdown"),
            F.avg("avg_spread_bps").alias("avg_spread_bps"),
        )
        .withColumn(
            "day_return_pct",
            (F.col("day_close") / F.col("day_open") - 1) * 100,
        )
    )


def run(overwrite: bool = True) -> dict[str, int]:
    """Job entrypoint: read silver, write gold tables."""
    spark = build_spark("gold")
    silver_ticks = read_table(spark, "silver", "ticks")
    silver_trades = read_table(spark, "silver", "trades")
    mode = "overwrite" if overwrite else "append"

    ohlcv = build_ohlcv_1m(silver_ticks, silver_trades)
    write_table(ohlcv, "gold", "ohlcv_1m", mode=mode, partition_by=["trade_date"])

    risk = build_symbol_risk_daily(ohlcv)
    write_table(risk, "gold", "symbol_risk_daily", mode=mode)

    counts = {"ohlcv_1m": ohlcv.count(), "symbol_risk_daily": risk.count()}
    logger.info("Gold complete: %s", counts)
    return counts


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
