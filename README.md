# MarketPulse — Real-Time Market Data Lakehouse

**A production-grade, end-to-end data platform for financial market data.**
Kafka → Spark Structured Streaming → Delta Lake (medallion) → dbt → Airflow → Postgres → Streamlit, with contract-driven data quality, full CI/CD, and Terraform for the AWS production design.

[![ci](https://github.com/saipavan333/marketpulse/actions/workflows/ci.yml/badge.svg)](https://github.com/saipavan333/marketpulse/actions/workflows/ci.yml)
![python](https://img.shields.io/badge/python-3.10-blue)
![spark](https://img.shields.io/badge/spark-3.5-orange)
![license](https://img.shields.io/badge/license-MIT-green)

```
                        ┌─────────────────────────────────────────────────────────────┐
                        │                        ORCHESTRATION                        │
                        │                    Apache Airflow (DAGs)                    │
                        │      hourly medallion · daily ops · DQ gates as tasks       │
                        └──────────────┬──────────────────────────────────────────────┘
                                       │ schedules & monitors
┌──────────┐   ┌───────┐   ┌──────────▼─────────┐   ┌──────────────────────────────┐
│ Market   │   │ Kafka │   │  Spark Structured  │   │      LAKEHOUSE (MinIO/S3)    │
│ Simulator├──►│ topics├──►│     Streaming      ├──►│  bronze ─► silver ─► gold    │
│ (ticks,  │   │ v1    │   │  (micro-batches,   │   │  raw      validated  OHLCV,  │
│  trades) │   │       │   │   checkpointed)    │   │  audit    deduped,   VaR,    │
└──────────┘   └───────┘   └────────────────────┘   │  log      quarantine vol     │
                                                    └──────┬───────────────────────┘
                                                           │ contracts enforced
                                                           │ between every layer
                              ┌────────────────────────────▼──┐
                              │   WAREHOUSE (Postgres/DuckDB) │
                              │   gold tables + ops.dq_results│
                              └──────┬────────────────────────┘
                                     │
                      ┌──────────────┼─────────────────┐
              ┌───────▼──────┐ ┌─────▼─────┐ ┌─────────▼────────┐
              │  dbt marts   │ │ Streamlit │ │  downstream      │
              │  + dbt tests │ │ dashboard │ │  consumers (BI)  │
              └──────────────┘ └───────────┘ └──────────────────┘
```

## Why this project exists

Market data platforms are the backbone of every trading firm: billions of ticks a day, hard correctness requirements, and consumers (quants, risk, surveillance) who cannot tolerate silent data loss. MarketPulse demonstrates how to build that class of system end to end — not as slideware, but as runnable code with tests, quality gates, and CI/CD.

**Everything runs on your laptop with one command, free.** The same code maps 1:1 onto AWS managed services (see `terraform/aws`).

## Quickstart

### 60-second demo (no Docker, no services)

```bash
git clone https://github.com/saipavan333/marketpulse && cd marketpulse
pip install -e ".[dev,spark,warehouse]"
make demo     # simulate -> bronze -> silver -> contracts -> gold -> warehouse -> report
```

You'll watch 36,000 events flow through every layer of the medallion architecture, see dirty records quarantined with reasons, contracts enforced, and a per-symbol risk report printed from DuckDB.

### Full platform (Docker)

```bash
make up        # Kafka, MinIO, Spark cluster, Airflow, Postgres, dashboard
make produce   # stream simulated market events into Kafka
make stream    # Spark Structured Streaming: Kafka -> bronze Delta tables
# then enable the `marketpulse_medallion_hourly` DAG in Airflow
```

| UI | URL | Credentials |
|---|---|---|
| Airflow | http://localhost:8080 | admin / admin |
| MinIO console | http://localhost:9001 | minioadmin / minioadmin |
| Spark master | http://localhost:8081 | — |
| Dashboard | http://localhost:8501 | — |

## What's inside

| Capability | Implementation | Where |
|---|---|---|
| Realistic data | GBM price paths, U-shaped intraday volume, injected anomalies | `src/marketpulse/generator/` |
| Streaming ingestion | Kafka (idempotent producer) + Spark Structured Streaming with checkpoints & backpressure | `producer/`, `streaming/` |
| Medallion lakehouse | bronze (immutable audit) → silver (validated, deduped, quarantined) → gold (OHLCV, VaR, vol, drawdown) | `batch/` |
| Data quality | **Contract-driven DQ engine**: YAML contracts, severity levels, quarantine-not-drop, audit table | `quality/`, `contracts/` |
| Analytics engineering | dbt staging + marts with tests, seeds, source freshness | `dbt/marketpulse_dbt/` |
| Orchestration | Airflow DAGs with DQ gates as first-class tasks, SLAs, idempotent reruns | `dags/` |
| Serving | Postgres warehouse + Streamlit dashboard (candlesticks, risk, DQ panel) | `dashboards/` |
| CI/CD | GitHub Actions: lint → mypy → unit → **true end-to-end pipeline run** → dbt parse → image build/publish on tags | `.github/workflows/` |
| IaC | Terraform: MSK, EMR Serverless, MWAA, S3 lifecycle, RDS | `terraform/aws/` |
| Local/prod parity | Identical transform code; storage/format/endpoints switch via config | `config.py`, `utils/spark.py` |

## Engineering decisions worth reading

Each significant decision is captured as an ADR in [`docs/adr/`](docs/adr/):

1. **Parquet locally, Delta in docker/prod** — tests need zero infra; prod needs ACID + time travel.
2. **Quarantine, never drop** — invalid rows are diverted with a reason and audited; silent data loss is the cardinal sin of market data.
3. **Hand-rolled contracts engine (~150 lines) over Great Expectations** — shows what DQ tools do under the hood; swappable later.
4. **Pandas hop for warehouse loads** — gold aggregates are small; Spark JDBC writers are reserved for when row counts justify them.

Full documentation:

- [Architecture deep-dive](docs/architecture.md) — every component, every decision, scaling story
- [Data model](docs/data-model.md) — schemas, partitioning, lineage layer by layer
- [Runbook](docs/runbook.md) — operating, debugging, and recovering the platform
- [Project report](docs/PROJECT_REPORT.md) — goals, results, benchmarks, lessons learned
- [Learning guide](docs/LEARNING_GUIDE.md) — **study companion: concepts, interview Q&A, exercises**

## Testing

```bash
make test               # unit tests (generator, contracts engine, transforms)
make test-integration   # full pipeline end-to-end + idempotency proof
make lint typecheck     # ruff + mypy
```

CI runs all of the above on every PR, plus the complete pipeline on a clean runner — if the badge is green, `git clone` + `make demo` works.

## Repo layout

```
marketpulse/
├── src/marketpulse/        # python package (generator, producer, streaming, batch, quality)
├── contracts/              # YAML data contracts enforced between layers
├── dags/                   # Airflow DAGs (medallion hourly, ops daily)
├── dbt/marketpulse_dbt/    # dbt project: staging views + tested marts
├── dashboards/             # Streamlit app
├── docker/                 # app / spark / airflow images
├── terraform/aws/          # production reference architecture
├── tests/                  # unit + integration (pytest)
├── scripts/                # run_local_pipeline.py, warehouse bootstrap
└── docs/                   # architecture, data model, runbook, report, ADRs, learning guide
```

## License

MIT — use it, fork it, learn from it.

---

*All market data is synthetic (seeded GBM simulation). No exchange data licences were harmed in the making of this project.*
