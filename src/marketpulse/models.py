"""Event schemas shared by every layer of the platform.

These pydantic models are the **single source of truth** for what a tick
and a trade look like. The Kafka producer serialises them, the Spark
streaming job mirrors them as a StructType, and the data contracts in
``contracts/`` assert them at rest.

Schema evolution policy (interview talking point):
    - Topics are versioned (``market.ticks.v1``). Breaking changes ship as
      a new topic + new bronze table, never an in-place mutation.
    - Additive optional fields are allowed within a version.
"""

from __future__ import annotations

from enum import Enum

from pydantic import BaseModel, Field


class Side(str, Enum):
    BUY = "BUY"
    SELL = "SELL"


class OrderType(str, Enum):
    MARKET = "MARKET"
    LIMIT = "LIMIT"


class Tick(BaseModel):
    """A top-of-book quote snapshot for one symbol on one venue."""

    event_type: str = "tick"
    symbol: str
    venue: str
    seq: int = Field(description="Per-symbol monotonically increasing sequence number")
    event_ts: str = Field(description="Event time, ISO-8601 UTC (when the quote happened)")
    bid: float | None = Field(default=None, description="Best bid price")
    ask: float | None = Field(default=None, description="Best ask price")
    bid_size: int | None = None
    ask_size: int | None = None
    last: float | None = Field(default=None, description="Last traded price")


class Trade(BaseModel):
    """An executed trade print."""

    event_type: str = "trade"
    trade_id: str
    symbol: str
    venue: str
    event_ts: str
    price: float | None = None
    qty: int | None = None
    side: Side
    order_type: OrderType
    trader_id: str = Field(description="Anonymised counterparty id")


# Reference data: symbol -> (name, sector, base price, annualised volatility)
SYMBOL_UNIVERSE: dict[str, tuple[str, str, float, float]] = {
    "AAPL": ("Apple Inc.", "Technology", 232.0, 0.25),
    "MSFT": ("Microsoft Corp.", "Technology", 451.0, 0.22),
    "NVDA": ("NVIDIA Corp.", "Technology", 138.0, 0.45),
    "AMZN": ("Amazon.com Inc.", "Consumer", 215.0, 0.30),
    "GOOG": ("Alphabet Inc.", "Technology", 196.0, 0.26),
    "META": ("Meta Platforms", "Technology", 605.0, 0.32),
    "TSLA": ("Tesla Inc.", "Automotive", 342.0, 0.55),
    "GS": ("Goldman Sachs Group", "Financials", 612.0, 0.28),
    "JPM": ("JPMorgan Chase & Co.", "Financials", 265.0, 0.24),
    "MS": ("Morgan Stanley", "Financials", 138.0, 0.27),
}

VENUES: tuple[str, ...] = ("NYSE", "NASDAQ", "ARCA", "IEX")
