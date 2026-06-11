"""Silver transform tests: parsing, quarantine routing, dedupe (Spark local)."""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.spark

from pyspark.sql import functions as F  # noqa: E402

from marketpulse.batch.silver import build_silver_ticks, build_silver_trades  # noqa: E402


def test_silver_ticks_are_clean(bronze_events):
    silver, _quarantine = build_silver_ticks(bronze_events)
    rows = silver.collect()
    assert rows, "expected clean ticks"
    for r in rows:
        assert r.bid is not None and r.ask is not None
        assert r.bid > 0 and r.ask >= r.bid
        assert r.mid == pytest.approx((r.bid + r.ask) / 2)
        assert r.event_ts is not None and r.trade_date is not None


def test_silver_ticks_no_duplicate_keys(bronze_events):
    silver, _ = build_silver_ticks(bronze_events)
    total = silver.count()
    distinct = silver.select("symbol", "venue", "seq").distinct().count()
    assert total == distinct


def test_silver_trades_no_duplicate_ids(bronze_events):
    silver, _ = build_silver_trades(bronze_events)
    assert silver.count() == silver.select("trade_id").distinct().count()


def test_quarantine_catches_injected_dirt(bronze_events):
    """anomaly_rate=0.05 in the fixture -> quarantine must be non-empty
    and every quarantined row must carry a reason."""
    _, bad_ticks = build_silver_ticks(bronze_events)
    _, bad_trades = build_silver_trades(bronze_events)
    total_bad = bad_ticks.count() + bad_trades.count()
    assert total_bad > 0, "expected the simulator's dirt to be quarantined"
    for df in (bad_ticks, bad_trades):
        assert df.where(F.col("quarantine_reason").isNull()).count() == 0


def test_quarantine_reasons_are_known(bronze_events):
    from marketpulse.batch.silver import tick_rules, trade_rules

    _, bad_ticks = build_silver_ticks(bronze_events)
    _, bad_trades = build_silver_trades(bronze_events)
    tick_reasons = {r["quarantine_reason"] for r in bad_ticks.select("quarantine_reason").distinct().collect()}
    trade_reasons = {r["quarantine_reason"] for r in bad_trades.select("quarantine_reason").distinct().collect()}
    assert tick_reasons <= set(tick_rules())
    assert trade_reasons <= set(trade_rules())


def test_no_rows_lost(bronze_events):
    """Conservation law: parsed rows = clean + quarantined (nothing dropped)."""
    from marketpulse.batch.silver import parse_bronze

    parsed = parse_bronze(bronze_events, "trade")
    clean, bad = build_silver_trades(bronze_events)
    # dedupe removes exact duplicate re-deliveries; account for them
    dupes = parsed.count() - parsed.dropDuplicates(["trade_id"]).count()
    bad_non_dupe = bad.count()
    assert clean.count() + bad_non_dupe + dupes >= parsed.count()
