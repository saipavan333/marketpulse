# Learning Guide — Master This Project, Master the Interview

This guide turns MarketPulse from "a repo you own" into "a system you can defend for 60 minutes against a Goldman Sachs staff engineer." Work through it in order. Budget: ~2 focused weeks.

---

## Part 1 — Study path (read the code in this order)

**Day 1–2: The spine.** Run `make demo` first; watch the 8 stages. Then read `scripts/run_local_pipeline.py` top to bottom — it is the entire platform in one file. Trace each import it makes. You should be able to draw the architecture diagram from memory afterwards.

**Day 3: Data generation.** `generator/market_simulator.py`. Understand GBM (`_step_price`): why `exp(drift + sigma*shock)`? Why is drift `-0.5σ²` (answer: makes the *expected* price flat — a martingale — so the simulation doesn't trend artificially)? Why inject anomalies deliberately? Re-derive the annualisation constant.

**Day 4: Ingestion.** `producer/kafka_producer.py` and `streaming/bronze_stream.py`. Be able to explain: idempotent producer (what the broker dedupes and what it cannot), why messages are keyed by symbol, what the checkpoint directory contains, what `maxOffsetsPerTrigger` protects you from, why bronze stores unparsed JSON.

**Day 5–6: The medallion.** `batch/silver.py` then `batch/gold.py`. For silver: why explicit schemas (vs inference), how `split_quarantine` builds the reason column with chained `F.when`, how `dedupe` uses a window + `row_number`. For gold: how OHLC comes from window functions vs groupBy, why volume joins from trades (left join + fillna), each risk metric's formula.

**Day 7: Quality.** `quality/contracts.py` + `quality/checks.py` + the YAML files. Then `dags/` to see contracts as DAG gates. This is your differentiator — most candidates talk about data quality; you built an engine.

**Day 8: Serving.** `batch/warehouse.py`, the dbt project, `dashboards/app.py`. Understand the staging/marts split and every dbt test in `schema.yml`.

**Day 9: Operations.** `docker-compose.yml` service by service, `Makefile`, the CI workflow job by job, `terraform/aws/main.tf` resource by resource, the runbook end to end.

**Day 10: Re-build from memory.** Delete a file (e.g. `silver.py`), rewrite it without looking, diff. Repeat for whatever felt shaky.

## Part 2 — Core concepts you must own (with the answers)

### Exactly-once: the layered story
No single component gives you exactly-once; the system does. (1) Producer idempotence removes *our* retry duplicates at the broker. (2) Spark checkpoints Kafka offsets atomically with the micro-batch, so restarts re-read, never skip. (3) Replays therefore re-deliver — which is *fine*, because (4) silver dedupes on business keys, and (5) loads are overwrite/transactional. Memorise the phrase: **at-least-once delivery + idempotent processing = effectively-once.**

### Event time vs processing time
`event_ts` is when the market event happened; `ingested_at` is when we saw it. Late events (simulator injects them) land in the correct `trade_date` partition because partitioning derives from event time. In a fully streaming gold you'd add watermarks: "accept events up to X minutes late; later ones go to a late-events table." Be ready to write `withWatermark("event_ts", "10 minutes")` on a whiteboard.

### Why medallion at all?
Bronze = cheap insurance and a replay source (storage is cheap; lost data is not). Silver = one place where validation/dedupe happens, so every consumer inherits it. Gold = small, fast, business-shaped. The alternative — each consumer parsing raw — multiplies cost and divergent "truths."

### Partitioning strategy
Bronze by `ingest_date, kafka_topic` (write pattern + replay pattern). Silver/gold by `trade_date` (query pattern). Not by symbol: 10 symbols × small files = the small-files problem; symbol is a filter column, date is the partition. Know the failure mode of over-partitioning.

### The small-files problem
Streaming creates a file per micro-batch per partition. Over a day: thousands of tiny parquet files → slow listings, slow reads. Fix: daily `OPTIMIZE` (compaction) in the ops DAG; in plain parquet, periodic coalesce-rewrite. This question separates people who've run Spark from people who've read about it.

### Shuffle, skew, and why we set shuffle.partitions=8 locally
Wide transformations (groupBy, window, join) shuffle. Defaults (200 partitions) are wrong for small local data and often wrong for big data. Skew answer: salting hot keys, AQE skew-join handling (`spark.sql.adaptive` is on in `build_spark`), broadcast joins for small dims.

### The risk metrics (know the formulas cold)
- **VWAP** = Σ(price·qty)/Σ(qty). Execution benchmark: "did I trade better than the average market participant?"
- **Realised vol** = stddev(minute log returns) × √(252·390). Log returns because they compound additively.
- **Historical VaR(95)** = 5th percentile of the return distribution. "The minute-loss you exceed only 5% of the time." Limits: history ≠ future, fat tails.
- **Max drawdown** = min(close/running_peak − 1). Path-dependent — that's why it needs a running window, not a groupBy.

### Idempotency (their favourite word)
Definition: f(f(x)) = f(x) — rerunning produces the same state. Implemented here by: deterministic seeded input, overwrite semantics per window, keyed dedupe, delete-then-insert loads. Proven by `test_pipeline_is_idempotent`. When asked "how do you make a pipeline safe to retry?", this is the answer structure.

## Part 3 — Interview Q&A drill (answer aloud, then check)

**Q: Walk me through your project in 2 minutes.**
Structure: problem (market data with correctness guarantees) → flow (Kafka → streaming bronze → validated silver → gold risk metrics → warehouse/dbt/dashboard) → three differentiators (contract engine as DAG gates; quarantine pattern; CI that runs the whole platform per PR) → scale story (config-switched: laptop/Docker/AWS).

**Q: Your streaming job died Friday 8pm and nobody noticed until Monday. What happens?**
Nothing is lost: Kafka retains the weekend (retention 24h in compose — admit it, and say prod would be 7d+ or tiered). On restart, checkpointed offsets resume; `maxOffsetsPerTrigger` drains the backlog at a bounded rate; silver dedupes any replay overlap; SLA alerts (and in prod, lag monitoring on the consumer group) are the detection fix.

**Q: A quant says yesterday's NVDA vol number looks wrong. Debug it.**
Walk the lineage backwards: mart → gold `symbol_risk_daily` → `ohlcv_1m` bars (any insane bars? spike prints?) → silver ticks (check quarantine — did a fat-finger get *through*?) → contracts (`ops.dq_results` for that day) → bronze (is the raw feed itself bad?). Then state the fix path: patch rule/contract, replay the day (idempotent), publish corrected data + an incident note. Interviewers want the *systematic backwards walk*, not a guess.

**Q: Why Kafka and not just writing files to S3?**
Decoupling (N consumers at their own pace), replay (offset rewind), ordering per key, backpressure absorption during downstream outages. Counter-question to anticipate: "when WOULD files be enough?" — batch-only sources, single consumer, latency in hours.

**Q: Why didn't you use Great Expectations / Glue / a managed thing?**
Use the ADR: scope didn't justify the weight; contracts stay declarative and portable; demonstrating the internals was the point. Showing you *evaluated* (criteria in ADR-0003) is the senior signal — never trash the tool.

**Q: How would you add a new asset class (options)?**
New topic `market.options.v1` + pydantic model + explicit silver schema + new contract + gold greeks table. Existing tables untouched — additive evolution. Mention: shared symbol dimension, different validation rules (negative rates/IV bounds).

**Q: 100× the data. What breaks first?**
In order: (1) single Spark worker — scale workers; (2) gold pandas hop — switch per ADR-0004 criteria to JDBC/COPY; (3) Postgres as warehouse — Redshift/Snowflake/ClickHouse; (4) per-day full rebuilds — incremental (MERGE) silver; (5) Airflow LocalExecutor — Celery/K8s executor. Naming the *order* shows you've thought about bottlenecks, not memorised tools.

**Q: Why is your bronze immutable? GDPR says delete user X.**
Trades carry `trader_id` (pseudonymous). Real answer: crypto-shredding (encrypt PII columns with per-entity keys; delete the key) or rewrite-with-tombstones via Delta `DELETE` + `VACUUM`. Immutability is a default, not a religion.

## Part 4 — Hands-on exercises (do these before interviewing)

1. **Add a contract check type** (`stddev_below`): implement in `checks.py`, use it on `spread_bps`, write the unit test. (~1h — proves you can extend the engine live if asked.)
2. **Break it on purpose:** set `anomaly_rate=0.5`, run the pipeline, watch contracts fail, read the quarantine report, fix by tightening producer-side validation. Narrate the incident as you would in a postmortem.
3. **Add a symbol** (e.g. NFLX) end to end: universe → seed CSV → rerun → verify it appears in the mart. Map every file you touched; that's your lineage story.
4. **Streaming gold:** write `streaming/gold_stream.py` computing 1-minute bars with `window()` + `withWatermark` directly from a tick stream. Compare results against batch gold for the same data — they should match (and discovering why they slightly don't, at the watermark boundary, is the best interview story in this repo).
5. **Benchmark:** generate 50× the events, measure each stage, find the slowest, fix one thing (partition count? broadcast join?), measure again. Numbers beat adjectives in interviews.

## Part 5 — How to present this on your profile

Pin the repo. The README badge must be green (set up Actions on first push). In interviews, drive to your strengths: "want me to show you how the contract engine gates the DAG?" Your CV bullet: *"Built a production-grade market-data lakehouse (Kafka, Spark, Delta, dbt, Airflow) with a contract-driven data-quality engine and CI that executes the full pipeline on every PR — 30+ tests, idempotency proven, AWS design in Terraform."*

When asked "is this production experience?", the honest, strong answer: "It's a personal platform built to production standards — quality gates, idempotency, CI/CD, runbooks. At CGI my production work is X; this repo is where I demonstrate decisions end-to-end because I own every layer."
