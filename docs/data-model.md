# Data Model

Layer-by-layer schemas, keys, partitioning, and lineage. The diagrams below read top-to-bottom as data flows.

## Lineage overview

```
market.ticks.v1 ─┐                                  ┌─ gold/ohlcv_1m ──► marts.fct_intraday_liquidity
                 ├─► bronze/events ─► silver/ticks ─┤                └─► marts.fct_daily_symbol_performance
market.trades.v1─┘        │      └──► silver/trades─┤
                          │                         └─ gold/symbol_risk_daily ─► marts.fct_daily_symbol_performance
                          │
                          └────► silver/quarantine_{ticks,trades}   (+ ops.dq_results audit)
```

## Kafka topics

| Topic | Key | Value | Partitions | Notes |
|---|---|---|---|---|
| `market.ticks.v1` | symbol | Tick JSON | 6 | per-symbol ordering within partition |
| `market.trades.v1` | symbol | Trade JSON | 6 | |
| `market.dlq.v1` | — | failed payloads | 1 | dead-letter destination |

**Tick payload** (`marketpulse.models.Tick`): `event_type, symbol, venue, seq, event_ts, bid, ask, bid_size, ask_size, last`.
**Trade payload** (`marketpulse.models.Trade`): `event_type, trade_id, symbol, venue, event_ts, price, qty, side, order_type, trader_id`.

## bronze/events

Immutable audit log. One row per Kafka message, value kept verbatim.

| column | type | meaning |
|---|---|---|
| raw_value | string | original JSON, untouched |
| kafka_topic | string | source topic |
| kafka_partition | int | source partition |
| kafka_offset | long | source offset (with topic+partition: unique message id) |
| kafka_ts | timestamp | broker receive time |
| ingested_at | timestamp | our processing time |
| ingest_date | date | **partition column** |

Partitioned by `ingest_date, kafka_topic`. Retention: 90 days hot, then Glacier (terraform lifecycle rule).

## silver/ticks

One row per (symbol, venue, seq) — validated, deduplicated, typed.

| column | type | constraint (contract-enforced) |
|---|---|---|
| symbol | string | not null |
| venue | string | not null, in {NYSE, NASDAQ, ARCA, IEX} |
| seq | bigint | not null; unique with symbol+venue |
| event_ts | timestamp | not null (event time, NOT ingest time) |
| bid, ask | double | not null, > 0, ask >= bid |
| bid_size, ask_size | int | |
| last | double | |
| mid | double | derived: (bid+ask)/2 |
| spread_bps | double | derived: (ask-bid)/mid * 10^4; warn if > 500 |
| kafka_offset, ingested_at | | provenance |
| trade_date | date | **partition column** (from event_ts) |

## silver/trades

One row per trade_id.

| column | type | constraint |
|---|---|---|
| trade_id | string | not null, unique |
| symbol, venue | string | not null; venue in accepted set |
| event_ts | timestamp | not null |
| price | double | not null, > 0 |
| qty | int | not null, >= 1 |
| side | string | BUY or SELL |
| order_type | string | MARKET or LIMIT |
| trader_id | string | anonymised counterparty |
| notional | double | derived: price * qty |
| trade_date | date | **partition column** |

## silver/quarantine_{ticks,trades}

Same parsed columns as their clean counterparts plus `quarantine_reason` (string, never null): one of `null_symbol, null_prices, non_positive_price, crossed_quote, bad_timestamp, null_trade_id, null_price, null_qty, non_positive_qty`. Quarantine preserves the rejected evidence — auditable, replayable, alertable.

## gold/ohlcv_1m

One row per (symbol, minute). OHLC computed from tick mids; volume/VWAP from real trades.

| column | type | definition |
|---|---|---|
| symbol, minute | string, timestamp | grain key |
| open, high, low, close | double | first/max/min/last mid within the minute |
| avg_spread_bps | double | mean quoted spread |
| tick_count | long | quote updates in the bar |
| volume | bigint | sum of trade qty (0 if no trades) |
| notional | double | sum of price*qty |
| vwap | double | sum(price*qty)/sum(qty), null if no trades |
| trade_count | bigint | trades in the bar |
| trade_date | date | **partition column** |

Invariant (tested + contract-enforced): `low <= open, close <= high`.

## gold/symbol_risk_daily

One row per (symbol, trade_date).

| column | definition |
|---|---|
| day_open/high/low/close | session OHLC from minute bars |
| day_return_pct | (close/open - 1) * 100 |
| total_volume, total_notional, trade_count | session activity |
| realised_vol_annualised | stddev(minute log returns) × √(252×390) |
| var_95_log_ret | 5th percentile of minute log returns (historical VaR) |
| max_drawdown | min over the day of close/running_peak − 1 (≤ 0 by construction) |
| avg_spread_bps | mean of bar spreads |

## Warehouse (Postgres) schemas

| schema | contents | written by |
|---|---|---|
| `gold` | ohlcv_1m, symbol_risk_daily (mirrors of lake gold) | load_warehouse task |
| `analytics_staging` | dbt staging views | dbt |
| `analytics_marts` | dim_symbol, fct_daily_symbol_performance, fct_intraday_liquidity | dbt |
| `analytics_reference` | symbol_reference seed | dbt seed |
| `ops` | dq_results (every contract check ever run), pipeline_runs | DQ engine / jobs |

## Schema evolution policy

1. **Additive optional column**: allowed within topic version; bronze unaffected (raw JSON); silver schema + contract updated in the same PR (CI enforces both).
2. **Breaking change** (rename, type change, semantic change): new topic version `v2`, new bronze/silver tables, dual-run during migration, then retire v1. Never mutate history in place.
3. Contracts are the change-review artifact: a PR that touches a schema must touch the contract, which reviewers treat as the API diff.
