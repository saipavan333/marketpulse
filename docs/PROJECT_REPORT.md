# MarketPulse — Project Report

**Author:** U E Sai Pavan Vamshi Krishna
**Repository:** https://github.com/saipavan333/marketpulse
**Date:** June 2026
**Status:** v1.0 — complete, all tests green

---

## 1. Executive summary

MarketPulse is a production-grade, end-to-end data platform for financial market data, built to the standard expected of a senior data engineer at a top-tier financial institution. It ingests a simulated exchange feed (quotes and trades) through Kafka, lands it in a medallion lakehouse via Spark Structured Streaming, refines it through validated batch layers, enforces declarative data contracts between every layer, and serves risk analytics (OHLCV bars, realised volatility, VaR, drawdown) through a Postgres warehouse, dbt marts, and a Streamlit dashboard — all orchestrated by Airflow, tested end-to-end, and shipped through a CI/CD pipeline that runs the complete platform on every pull request.

The entire system runs on a laptop with one command (`make demo`, no Docker required) and scales by configuration — not code change — to a Docker stack and to an AWS reference architecture expressed in Terraform.

## 2. Objectives and success criteria

| Objective | Result |
|---|---|
| End-to-end pipeline: ingest → lake → quality → analytics → serving | ✅ 8-stage pipeline, single command, ~20s at demo scale |
| Streaming + batch (lambda-style medallion) from one codebase | ✅ shared transforms; streaming bronze, batch silver/gold |
| Data quality as enforced gates, not reports | ✅ contract engine; error-severity failures block the DAG |
| Zero silent data loss | ✅ quarantine-with-reason pattern; conservation tested |
| Reproducibility | ✅ seeded simulator; idempotency proven by integration test |
| CI/CD | ✅ lint, types, 32+ unit tests, full e2e run, dbt parse, image publish on tags |
| Documented decisions | ✅ 4 ADRs, architecture deep-dive, runbook, data model |

## 3. Architecture (summary)

```
Simulator → Kafka (idempotent producer, keyed by symbol)
          → Spark Structured Streaming (checkpointed, bounded micro-batches)
          → bronze (raw + ingest metadata; immutable audit log)
          → silver (explicit schemas, validate → quarantine → dedupe)
          → gold (OHLCV 1-minute bars, daily risk metrics)
          → warehouse (Postgres / DuckDB)
          → dbt (staging views + tested marts) → dashboard
Airflow orchestrates; data contracts gate every hop; everything idempotent.
```

Full detail: `docs/architecture.md`. Schemas and lineage: `docs/data-model.md`.

## 4. Key engineering decisions

**Contract-driven data quality.** Every table has a YAML contract (columns, types, nullability, uniqueness, ranges, freshness, min-rows) executed by a purpose-built ~150-line engine. Failures at `error` severity raise and stop the pipeline before bad data propagates; all results are persisted to `ops.dq_results` for trend observability. Rationale and trade-offs vs Great Expectations: ADR-0003.

**Quarantine, never drop.** Silver routes invalid rows to quarantine tables with a `quarantine_reason`. A conservation test asserts parsed = clean + quarantined. The ops DAG alerts when the quarantine rate exceeds 5%. Rationale: ADR-0002.

**Local/prod parity through configuration.** The same transform functions run against local Parquet (tests, demo), Dockerised Delta-on-MinIO (full stack), and S3/EMR (Terraform design). Format and endpoints are pydantic-settings config. Rationale: ADR-0001.

**Idempotency everywhere.** Producer idempotence; checkpointed streaming offsets; keyed dedupe in silver; overwrite/delete-then-insert loads. The integration suite re-runs the pipeline and asserts byte-identical gold output.

## 5. Verification evidence

Executed on a clean Linux environment (Python 3.10, Java 11, Spark 3.5.1 local mode):

```
tests/unit/test_generator.py        8 passed   (determinism, realism bounds, anomaly injection)
tests/unit/test_contracts.py        4 passed   (contract parsing & validation)
tests/unit/test_quality_engine.py   9 passed   (every check type, severity semantics)
tests/unit/test_silver.py           6 passed   (parsing, quarantine routing, dedupe, conservation)
tests/unit/test_gold.py             5 passed   (OHLC invariants, hand-computed VWAP, risk sanity)
ruff check                          All checks passed
scripts/run_local_pipeline.py       exit 0 — "Pipeline complete in 20.0s"
                                    74/74 data-contract checks passed
                                    0 OHLC invariant violations in warehouse
```

Sample output (single-symbol demo run):

```
  Daily risk summary:
  symbol       close    ret%    vol%   maxDD%      volume   trades
  GS          612.73    0.11     9.9    -0.03      14,387       84

  DQ audit: 74/74 checks passed (run 3e40f740)
  Pipeline complete in 20.0s
```

## 6. What I learned / would do differently

**Module-level Spark expressions are a trap.** `F.col()` requires an active SparkContext; defining validation rules as module constants broke imports in context-free environments. Refactored to lazy factory functions — a lesson about keeping import time side-effect free that applies to any Spark codebase.

**Drive quality from injected failure.** Building the simulator to *deliberately* produce duplicates, nulls, crossed quotes and fat-finger spikes made the quality layer testable and demonstrable rather than theoretical — the quarantine and contracts demonstrably catch real defect classes on every run.

**Aggregate size dictates load strategy.** Gold tables are small; a transactional pandas/SQLAlchemy hop beats a Spark JDBC writer for simplicity — with documented switch criteria (ADR-0004). Knowing when *not* to use big-data tooling is part of the job.

**Next iterations:** move OHLCV computation into the streaming layer with watermarked windows for sub-minute latency; add OpenLineage emission for column-level lineage; add a Flink implementation of the same pipeline as a comparison study; wire alerting (Slack webhook) into DQ failures.

## 7. Skills demonstrated (mapping to role expectations)

Streaming ingestion (Kafka, Spark Structured Streaming, checkpointing, backpressure, exactly-once reasoning); lakehouse design (medallion, Delta/Parquet, partitioning, compaction); batch processing (PySpark window functions, aggregations); data modelling (facts/dims, dbt staging/marts); data quality engineering (contracts, severity, quarantine, audit); orchestration (Airflow DAG design, idempotent tasks, SLAs); financial analytics (OHLCV, VWAP, realised vol, historical VaR, drawdown); software engineering (typed Python, pure functions, pytest incl. integration & idempotency tests); DevOps (Docker multi-service stack, GitHub Actions CI/CD, image publishing); cloud architecture (Terraform: MSK, EMR Serverless, MWAA, S3 lifecycle, RDS with Secrets Manager).
