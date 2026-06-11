"""Realistic synthetic market data simulator.

Why synthetic? (interview talking point)
    Real exchange feeds cost money and are licence-restricted. A seeded
    simulator gives **deterministic, reproducible** data with controllable
    volume — perfect for testing pipelines, demoing failure modes, and CI.

What makes it realistic:
    - Prices follow Geometric Brownian Motion (GBM) per symbol, calibrated
      with a per-symbol annualised volatility from reference data.
    - Intraday U-shaped activity curve (busy open/close, quiet lunch).
    - Bid/ask spread widens with volatility; sizes are log-normal-ish.
    - Trades print around the touch with realistic side imbalance.

Deliberate dirt (this is what the data-quality layer exists to catch):
    - duplicate events (same seq / trade_id re-delivered)
    - null prices / null quantities
    - impossible values (negative qty, crossed quotes where ask < bid)
    - extreme price spikes (fat-finger style, > 10 sigma)
    - late events (event_ts minutes behind wall-clock arrival)
"""

from __future__ import annotations

import math
import random
import uuid
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

from marketpulse.models import SYMBOL_UNIVERSE, VENUES, OrderType, Side, Tick, Trade

TRADING_SECONDS_PER_YEAR = 252 * 6.5 * 3600  # 252 sessions x 6.5h


@dataclass
class SimulatorConfig:
    symbols: list[str] = field(default_factory=lambda: list(SYMBOL_UNIVERSE))
    seed: int = 42
    anomaly_rate: float = 0.005  # fraction of events that are intentionally dirty
    trade_to_tick_ratio: float = 0.35  # roughly 1 trade per ~3 ticks
    start: datetime | None = None  # simulation clock start (UTC)


class MarketSimulator:
    """Deterministic event-stream generator for ticks and trades."""

    def __init__(self, config: SimulatorConfig | None = None) -> None:
        self.config = config or SimulatorConfig()
        unknown = [s for s in self.config.symbols if s not in SYMBOL_UNIVERSE]
        if unknown:
            raise ValueError(f"Unknown symbols: {unknown}. Choose from {list(SYMBOL_UNIVERSE)}")
        self._rng = random.Random(self.config.seed)
        self._clock = self.config.start or datetime(2026, 6, 11, 13, 30, tzinfo=timezone.utc)
        self._prices: dict[str, float] = {
            s: SYMBOL_UNIVERSE[s][2] for s in self.config.symbols
        }
        self._seq: dict[str, int] = dict.fromkeys(self.config.symbols, 0)
        self._recent: list[dict] = []  # buffer used to emit duplicates

    # ------------------------------------------------------------------ core
    def _step_price(self, symbol: str, dt_seconds: float) -> float:
        """Advance one symbol's price with a GBM step."""
        sigma_annual = SYMBOL_UNIVERSE[symbol][3]
        sigma = sigma_annual * math.sqrt(dt_seconds / TRADING_SECONDS_PER_YEAR)
        drift = -0.5 * sigma * sigma  # zero-expected-return martingale
        shock = self._rng.gauss(0.0, 1.0)
        self._prices[symbol] *= math.exp(drift + sigma * shock)
        return self._prices[symbol]

    def _intraday_intensity(self) -> float:
        """U-shaped activity: 1.0 at open/close, ~0.4 mid-session."""
        minutes = (self._clock.hour * 60 + self._clock.minute) % (24 * 60)
        session_pos = max(0.0, min(1.0, (minutes - 13.5 * 60) / (6.5 * 60)))
        return 0.4 + 0.6 * (2 * session_pos - 1) ** 2

    def _spread_bps(self, symbol: str) -> float:
        """Spread in basis points, wider for more volatile names."""
        vol = SYMBOL_UNIVERSE[symbol][3]
        return self._rng.uniform(0.5, 2.0) + vol * 8

    # ------------------------------------------------------------- emitters
    def _make_tick(self, symbol: str) -> dict:
        price = self._step_price(symbol, dt_seconds=1.0)
        half_spread = price * self._spread_bps(symbol) / 10_000 / 2
        self._seq[symbol] += 1
        tick = Tick(
            symbol=symbol,
            venue=self._rng.choice(VENUES),
            seq=self._seq[symbol],
            event_ts=self._clock.isoformat(),
            bid=round(price - half_spread, 4),
            ask=round(price + half_spread, 4),
            bid_size=int(self._rng.lognormvariate(5.0, 1.0)) + 1,
            ask_size=int(self._rng.lognormvariate(5.0, 1.0)) + 1,
            last=round(price, 4),
        )
        return tick.model_dump()

    def _make_trade(self, symbol: str) -> dict:
        mid = self._prices[symbol]
        side = Side.BUY if self._rng.random() < 0.52 else Side.SELL
        slip = mid * self._rng.uniform(0, 3) / 10_000
        price = mid + slip if side is Side.BUY else mid - slip
        trade = Trade(
            trade_id=str(uuid.UUID(int=self._rng.getrandbits(128), version=4)),
            symbol=symbol,
            venue=self._rng.choice(VENUES),
            event_ts=self._clock.isoformat(),
            price=round(price, 4),
            qty=max(1, int(self._rng.lognormvariate(4.5, 1.2))),
            side=side,
            order_type=OrderType.MARKET if self._rng.random() < 0.7 else OrderType.LIMIT,
            trader_id=f"T{self._rng.randint(1, 500):04d}",
        )
        return trade.model_dump()

    # ------------------------------------------------------------ anomalies
    def _corrupt(self, event: dict) -> dict:
        """Inject one of six realistic data-quality defects."""
        kind = self._rng.choice(
            ["duplicate", "null_price", "null_qty", "negative_qty", "spike", "late"]
        )
        if kind == "duplicate" and self._recent:
            return dict(self._rng.choice(self._recent))  # exact re-delivery
        if kind == "null_price":
            for f in ("price", "bid", "ask", "last"):
                if f in event:
                    event[f] = None
        elif kind == "null_qty" and "qty" in event:
            event["qty"] = None
        elif kind == "negative_qty" and "qty" in event:
            event["qty"] = -abs(event["qty"] or 1)
        elif kind == "spike":
            factor = self._rng.choice([0.1, 12.0])  # crash print or fat-finger
            for f in ("price", "bid", "ask", "last"):
                if f in event and event[f] is not None:
                    event[f] = round(event[f] * factor, 4)
        elif kind == "late":
            late_ts = self._clock - timedelta(minutes=self._rng.randint(5, 45))
            event["event_ts"] = late_ts.isoformat()
        return event

    # -------------------------------------------------------------- public
    def stream(self, total_events: int) -> Iterator[dict]:
        """Yield ``total_events`` tick/trade events, advancing the sim clock."""
        emitted = 0
        while emitted < total_events:
            # advance wall clock ~1s, modulated by intraday intensity
            self._clock += timedelta(milliseconds=int(1000 / max(self._intraday_intensity(), 0.1)))
            for symbol in self.config.symbols:
                if emitted >= total_events:
                    break
                event = (
                    self._make_trade(symbol)
                    if self._rng.random() < self.config.trade_to_tick_ratio
                    else self._make_tick(symbol)
                )
                if self._rng.random() < self.config.anomaly_rate:
                    event = self._corrupt(event)
                self._recent.append(event)
                if len(self._recent) > 200:
                    self._recent.pop(0)
                emitted += 1
                yield event

    def minutes(self, minutes: int, events_per_minute: int = 600) -> Iterator[dict]:
        """Convenience: simulate N market-minutes of activity."""
        yield from self.stream(total_events=minutes * events_per_minute)
