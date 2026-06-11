"""Generator tests: determinism, realism bounds, anomaly injection."""

from __future__ import annotations

from marketpulse.generator import MarketSimulator, SimulatorConfig
from marketpulse.models import SYMBOL_UNIVERSE


def _events(seed: int = 1, n: int = 1000, anomaly_rate: float = 0.0) -> list[dict]:
    sim = MarketSimulator(
        SimulatorConfig(symbols=["AAPL", "GS"], seed=seed, anomaly_rate=anomaly_rate)
    )
    return list(sim.stream(n))


def test_deterministic_with_same_seed():
    assert _events(seed=99) == _events(seed=99)


def test_different_seeds_differ():
    assert _events(seed=1) != _events(seed=2)


def test_event_count_exact():
    assert len(_events(n=777)) == 777


def test_unknown_symbol_rejected():
    import pytest

    with pytest.raises(ValueError, match="Unknown symbols"):
        MarketSimulator(SimulatorConfig(symbols=["NOTREAL"]))


def test_clean_ticks_are_valid():
    ticks = [e for e in _events(anomaly_rate=0.0) if e["event_type"] == "tick"]
    assert ticks, "expected some ticks"
    for t in ticks:
        assert t["bid"] is not None and t["ask"] is not None
        assert 0 < t["bid"] < t["ask"], "quotes must be positive and uncrossed"
        assert t["bid_size"] > 0 and t["ask_size"] > 0


def test_clean_trades_are_valid():
    trades = [e for e in _events(anomaly_rate=0.0) if e["event_type"] == "trade"]
    assert trades, "expected some trades"
    for t in trades:
        assert t["price"] is not None and t["price"] > 0
        assert t["qty"] is not None and t["qty"] >= 1
        assert t["side"] in ("BUY", "SELL")
    ids = [t["trade_id"] for t in trades]
    assert len(ids) == len(set(ids)), "clean trades must have unique ids"


def test_prices_stay_near_base():
    """GBM over a short horizon should not wander far from base price."""
    events = _events(n=2000, anomaly_rate=0.0)
    base = SYMBOL_UNIVERSE["AAPL"][2]
    aapl_last = [e["last"] for e in events if e["event_type"] == "tick" and e["symbol"] == "AAPL"]
    assert all(0.5 * base < p < 2.0 * base for p in aapl_last)


def test_anomalies_are_injected():
    """With a high anomaly rate we must observe defects to feed DQ tests."""
    events = _events(n=5000, anomaly_rate=0.30)
    trades = [e for e in events if e["event_type"] == "trade"]
    ticks = [e for e in events if e["event_type"] == "tick"]

    has_null_price = any(t["price"] is None for t in trades) or any(t["bid"] is None for t in ticks)
    has_bad_qty = any(t["qty"] is not None and t["qty"] <= 0 for t in trades)
    ids = [t["trade_id"] for t in trades]
    seqs = [(t["symbol"], t["seq"]) for t in ticks]
    has_duplicates = len(ids) != len(set(ids)) or len(seqs) != len(set(seqs))

    assert has_null_price, "expected null-price anomalies"
    assert has_bad_qty, "expected non-positive qty anomalies"
    assert has_duplicates, "expected duplicate events"
