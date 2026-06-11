# Architecture

This document explains how MarketPulse is put together, why each piece exists, and how the design scales from a laptop to production. Read it top-to-bottom once, then keep it open while reading the code.

## 1. The problem being solved

A market data platform ingests a continuous firehose of events (quotes and trades), guarantees their correctness, and serves derived analytics (bars, volatility, risk) to downstream consumers with low latency and full auditability. The hard requirements that shape every decision here:

**Correctness over speed.** A missing trade or a duplicated tick silently corrupts every number derived from it. The platform must be able to prove what it ingested, what it rejected, and why.

**Replayability.** Any day must be reproducible from raw data — for backfills after bug fixes, for regulator queries, for "what did we know at 14:32" questions.

**Late and disordered data.** Events arrive out of order (venue clock skew, network retries). Event time and processing time must be modelled separately.

**Scale asymmetry.** Raw events are huge (billions/day at a real firm); derived aggregates are tiny (thousands of rows). The architecture should treat them differently.

## 2. Layered design

### 2.1 Ingestion: simulator → Kafka

The `MarketSimulator` produces seeded, deterministic streams of ticks and trades using Geometric Brownian Motion per symbol, an intraday U-shaped activity curve, and **deliberately injected defects** (duplicates, nulls, negative quantities, fat-finger spikes, late events) at a configurable rate. Determinism matters: tests assert exact behaviour, demos are reproducible, and CI never flakes because of random data.

The Kafka producer is configured for safety first: `enable.idempotence=true` and `acks=all` mean broker-side retries cannot introduce duplicates from our side; messages are keyed by symbol so each symbol's events stay ordered within a partition; delivery callbacks count failures and the process exits non-zero if anything failed — an orchestrator sees a red task, never silence.

Topics are versioned (`market.ticks.v1`). A breaking schema change ships as `v2` alongside `v1`, consumers migrate, then `v1` is retired. Additive optional fields are allowed in-place.

### 2.2 Streaming: Kafka → bronze

A single Spark Structured Streaming query subscribes to both topics and lands every message in the bronze layer with ingestion metadata (topic, partition, offset, broker timestamp, ingest timestamp). Three deliberate choices:

**Bronze stores the raw JSON string, unparsed.** Parsing failures therefore cannot lose data — a malformed payload still lands in bronze and is quarantined during the silver build with a reason. Bronze is the platform's immutable audit log; with Delta enabled you also get time travel over it.

**Checkpointing + bounded micro-batches.** The query checkpoints offsets to durable storage, so a crashed job resumes exactly where it stopped. `maxOffsetsPerTrigger` bounds each micro-batch — a backlog after downtime is consumed at a controlled rate instead of OOM-ing the cluster.

**Partitioning by `ingest_date` and topic** keeps bronze scans pruned for the dominant query pattern ("reprocess yesterday's ticks").

### 2.3 Batch: bronze → silver → gold (medallion)

Silver parses bronze JSON against **explicit schemas** (never inferred — inference is non-deterministic and hides upstream contract breaks), then applies a validate → quarantine → dedupe sequence:

- Hard rules (null prices, crossed quotes, non-positive quantities, unparseable timestamps) divert rows to a quarantine table **with a reason column**. Nothing is dropped; quarantine rates are monitored and a >5% rate fails the ops DAG — that's a data incident, not a cleanup chore.
- Deduplication keeps the first-ingested row per business key (ticks: symbol+venue+seq; trades: trade_id). Combined with the idempotent producer this gives effectively-once semantics end to end — defence in depth rather than trusting any single layer.

Gold computes business aggregates: 1-minute OHLCV bars with VWAP from real trade volume, and daily per-symbol risk metrics — annualised realised volatility from minute log returns, historical VaR(95), max drawdown, spread averages. These are the tables quants and dashboards actually query.

All transforms are **pure functions** `DataFrame -> DataFrame`, separated from IO. That's what makes the test suite possible: unit tests construct tiny frames and assert exact numbers (e.g. hand-computed VWAP), without any lake or cluster.

### 2.4 Data quality: contracts between every layer

Every table has a YAML **data contract** (`contracts/`) declaring required columns, types, nullability, uniqueness, accepted values, ranges, freshness, and minimum row counts — each with a severity. A ~150-line engine executes contracts against Spark DataFrames; `error` failures raise and fail the Airflow task **before** downstream layers run, `warn` failures are logged and recorded. Every check result is appended to `ops.dq_results` in the warehouse, so quality is observable over time (the dashboard charts it).

Why not Great Expectations or Soda? Deliberate choice for a portfolio project: the engine demonstrates understanding of what those tools do internally, has zero heavy dependencies, and the declarative contracts would survive a later swap to GE/Soda unchanged. In a team context the trade-off would be revisited (ADR-0003).

### 2.5 Warehouse + analytics: gold → Postgres → dbt → dashboard

Gold aggregates are loaded into Postgres (DuckDB in local mode) — they're small, so a pandas hop is simpler and just as correct as a Spark JDBC writer (ADR-0004 documents when to switch). dbt then builds the analyst-facing layer: staging views (renames, typing — no business logic) and tested marts (`fct_daily_symbol_performance`, `fct_intraday_liquidity`, `dim_symbol`). dbt tests (unique, not_null, relationships, accepted ranges) are a second quality net at the serving layer, plus source freshness checks.

The Streamlit dashboard reads only the warehouse — it is a consumer like any other, downstream of all quality gates.

### 2.6 Orchestration: Airflow

The hourly medallion DAG wires the layers with **DQ gates as first-class tasks**:

```
bronze_to_silver ──► dq_silver_ticks ──┐
                 └─► dq_silver_trades ─┴─► silver_to_gold ─► dq_gold ─► load_warehouse ─► dbt_build
```

Every task is idempotent (full rebuild of its window), so retries and backfills are safe. The daily ops DAG produces a quarantine triage report (alerting if the rate exceeds threshold) and runs Delta `OPTIMIZE`/`VACUUM` — the unglamorous maintenance that keeps query latency flat as the lake grows.

## 3. Local/production parity

The entire platform runs three ways from the **same transform code**:

| | local (`make demo`) | docker (`make up`) | AWS (terraform) |
|---|---|---|---|
| Broker | file (simulated) | Kafka | MSK Serverless |
| Lake | local parquet | MinIO + Delta | S3 + Delta |
| Compute | Spark local[*] | Spark standalone cluster | EMR Serverless |
| Orchestrator | script | Airflow (LocalExecutor) | MWAA |
| Warehouse | DuckDB | Postgres | RDS / Redshift / Snowflake |

The switch is configuration (`MP_*` env vars via pydantic-settings), not code. This is the storage/compute separation argument made concrete: business logic must not know where bytes live.

## 4. Scaling story (the interview question)

**10× events?** Add Kafka partitions (keyed by symbol, so ordering is preserved per symbol), scale Spark workers; `maxOffsetsPerTrigger` already bounds micro-batches. Bronze/silver partitioning by date keeps reads pruned.

**1000× events (real exchange volume)?** Move OHLCV bar computation into the streaming job with watermarked windows (the gold logic is already window-based, so it ports); compact bronze aggressively; consider tiered storage on Kafka. The medallion contract — raw is sacred, silver is clean, gold is small — does not change.

**More consumers?** Gold and marts are the interface; consumers never touch bronze/silver. New use cases get new gold tables + contracts, not access to raw.

**Exactly-once?** Producer idempotence + checkpointed offsets + deterministic transforms + keyed dedupe in silver + idempotent (delete-then-insert / overwrite) loads = effectively-once end to end. Each layer assumes the others can fail.

## 5. Security & operations notes

Local credentials in compose are intentionally dummy; production uses IAM auth to MSK, KMS-encrypted S3, Secrets Manager for RDS (`manage_master_user_password`), private subnets throughout (see `terraform/aws`). CI never holds cloud credentials — image publishing uses the repo-scoped `GITHUB_TOKEN`. The runbook (`docs/runbook.md`) covers failure modes: checkpoint corruption, quarantine spikes, backfills, and schema evolution procedure.
