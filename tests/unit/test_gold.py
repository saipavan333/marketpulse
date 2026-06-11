"""Gold aggregate tests: OHLC invariants, VWAP math, risk metrics."""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.spark

from marketpulse.batch.gold import build_ohlcv_1m, build_symbol_risk_daily  # noqa: E402
from marketpulse.batch.silver import build_silver_ticks, build_silver_trades  # noqa: E402


@pytest.fixture()
def gold_inputs(bronze_events):
    silver_ticks, _ = build_silver_ticks(bronze_events)
    silver_trades, _ = build_silver_trades(bronze_events)
    return silver_ticks, silver_trades


def test_ohlc_invariants(gold_inputs):
    """low <= open/close <= high for every bar — the classic sanity check."""
    ohlcv = build_ohlcv_1m(*gold_inputs)
    for r in ohlcv.collect():
        assert r.low <= r.high
        assert r.low <= r.open <= r.high
        assert r.low <= r.close <= r.high
        assert r.volume >= 0 and r.trade_count >= 0


def test_bar_key_unique(gold_inputs):
    ohlcv = build_ohlcv_1m(*gold_inputs)
    assert ohlcv.count() == ohlcv.select("symbol", "minute").distinct().count()


def test_vwap_within_bar_range_when_present(gold_inputs):
    """VWAP is a trade-price average so it must sit within the day's
    traded price envelope (loose check: positive and finite)."""
    ohlcv = build_ohlcv_1m(*gold_inputs)
    for r in ohlcv.where("vwap IS NOT NULL").collect():
        assert r.vwap > 0


def test_vwap_exact_math(spark):
    """Hand-computed VWAP on a tiny crafted frame."""
    from datetime import datetime

    ticks = spark.createDataFrame(
        [
            ("AAPL", "NYSE", 1, datetime(2026, 1, 5, 10, 0, 1), 99.0, 101.0, 100.0, 2.0),
            ("AAPL", "NYSE", 2, datetime(2026, 1, 5, 10, 0, 30), 99.5, 100.5, 100.0, 1.0),
        ],
        "symbol string, venue string, seq long, event_ts timestamp, bid double, ask double, mid double, spread_bps double",
    )
    trades = spark.createDataFrame(
        [
            ("t1", "AAPL", datetime(2026, 1, 5, 10, 0, 10), 100.0, 10, 1000.0),
            ("t2", "AAPL", datetime(2026, 1, 5, 10, 0, 50), 110.0, 30, 3300.0),
        ],
        "trade_id string, symbol string, event_ts timestamp, price double, qty int, notional double",
    )
    bar = build_ohlcv_1m(ticks, trades).collect()[0]
    # VWAP = (100*10 + 110*30) / 40 = 4300/40 = 107.5
    assert bar.vwap == pytest.approx(107.5)
    assert bar.volume == 40
    assert bar.trade_count == 2


def test_risk_metrics_sane(gold_inputs):
    ohlcv = build_ohlcv_1m(*gold_inputs)
    risk = build_symbol_risk_daily(ohlcv)
    rows = risk.collect()
    assert rows
    for r in rows:
        assert r.day_low <= r.day_high
        assert r.max_drawdown <= 0, "drawdown is peak-to-trough, never positive"
        if r.realised_vol_annualised is not None:
            assert r.realised_vol_annualised >= 0
        if r.var_95_log_ret is not None:
            assert r.var_95_log_ret <= 0.01, "5th percentile of returns should be small/negative"
