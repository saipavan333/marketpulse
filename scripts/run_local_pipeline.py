#!/usr/bin/env python3
"""Run the ENTIRE MarketPulse pipeline locally — no Docker, no services.

    python scripts/run_local_pipeline.py --symbols AAPL,GS,NVDA --minutes 30

What happens (mirrors the production flow 1:1, same transform code):
    1. simulate   N market-minutes of ticks/trades (with injected dirt)
    2. bronze     land raw JSON + ingest metadata in the local lake
    3. silver     parse -> validate -> quarantine -> dedupe
    4. dq         enforce data contracts on silver tables
    5. gold       OHLCV bars + daily risk metrics
    6. dq         enforce contracts on gold
    7. warehouse  load gold into DuckDB
    8. report     print a summary straight from the warehouse

This script exists so anyone cloning the repo sees the platform work in
under two minutes — and so CI can run a true end-to-end test on every PR.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--symbols", default="AAPL,GS,NVDA")
    parser.add_argument("--minutes", type=int, default=30)
    parser.add_argument("--events-per-minute", type=int, default=400)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--workdir", default=str(REPO_ROOT / "data" / "local_run"))
    args = parser.parse_args()

    import os

    workdir = Path(args.workdir)
    workdir.mkdir(parents=True, exist_ok=True)
    # Local mode: parquet lake + duckdb warehouse inside workdir
    os.environ.setdefault("MP_ENV", "local")
    os.environ["MP_LAKE_ROOT"] = str(workdir / "lake")
    os.environ["MP_WAREHOUSE_URL"] = f"duckdb:///{workdir / 'marketpulse.duckdb'}"
    os.environ["MP_DELTA_ENABLED"] = "false"

    from marketpulse.batch.gold import build_ohlcv_1m, build_symbol_risk_daily
    from marketpulse.batch.silver import build_silver_ticks, build_silver_trades
    from marketpulse.batch.warehouse import load_gold_tables
    from marketpulse.generator import MarketSimulator, SimulatorConfig
    from marketpulse.quality import load_contract, run_contract
    from marketpulse.quality.checks import enforce, persist_results
    from marketpulse.utils.spark import build_spark, read_table, write_table

    t0 = time.time()
    symbols = [s.strip().upper() for s in args.symbols.split(",")]
    banner = lambda s: print(f"\n{'=' * 70}\n  {s}\n{'=' * 70}")  # noqa: E731

    # ---------------------------------------------------------- 1. simulate
    banner(f"1/8 SIMULATE {args.minutes} market-minutes for {symbols}")
    sim = MarketSimulator(SimulatorConfig(symbols=symbols, seed=args.seed))
    events_file = workdir / "events.jsonl"
    n_events = 0
    with events_file.open("w") as fh:
        for event in sim.minutes(args.minutes, events_per_minute=args.events_per_minute):
            fh.write(json.dumps(event) + "\n")
            n_events += 1
    print(f"  generated {n_events:,} events -> {events_file}")

    # ------------------------------------------------------------ 2. bronze
    banner("2/8 BRONZE: land raw events with ingest metadata")
    spark = build_spark("local-pipeline", local=True)
    from pyspark.sql import functions as F

    raw = spark.read.text(str(events_file)).withColumnRenamed("value", "raw_value")
    bronze = (
        raw.withColumn("kafka_topic", F.lit("local.file"))
        .withColumn("kafka_partition", F.lit(0))
        .withColumn("kafka_offset", F.monotonically_increasing_id())
        .withColumn("kafka_ts", F.current_timestamp())
        .withColumn("ingested_at", F.current_timestamp())
        .withColumn("ingest_date", F.to_date(F.current_timestamp()))
    )
    write_table(bronze, "bronze", "events", mode="overwrite")
    print(f"  bronze rows: {bronze.count():,}")

    # ------------------------------------------------------------ 3. silver
    banner("3/8 SILVER: parse -> validate -> quarantine -> dedupe")
    bronze_df = read_table(spark, "bronze", "events")
    silver_ticks, quarantine_ticks = build_silver_ticks(bronze_df)
    silver_trades, quarantine_trades = build_silver_trades(bronze_df)
    write_table(silver_ticks, "silver", "ticks", mode="overwrite", partition_by=["trade_date"])
    write_table(silver_trades, "silver", "trades", mode="overwrite", partition_by=["trade_date"])
    write_table(quarantine_ticks, "silver", "quarantine_ticks", mode="overwrite")
    write_table(quarantine_trades, "silver", "quarantine_trades", mode="overwrite")
    qt, qd = quarantine_ticks.count(), quarantine_trades.count()
    print(f"  silver ticks:  {silver_ticks.count():,}  (quarantined {qt})")
    print(f"  silver trades: {silver_trades.count():,}  (quarantined {qd})")
    if qt + qd:
        print("  quarantine reasons:")
        for layer_df in (quarantine_ticks, quarantine_trades):
            for row in layer_df.groupBy("quarantine_reason").count().collect():
                print(f"    - {row['quarantine_reason']}: {row['count']}")

    # ----------------------------------------------------- 4. dq on silver
    banner("4/8 DATA CONTRACTS on silver")
    all_results = []
    for table, contract_file in (("ticks", "silver_ticks.yml"), ("trades", "silver_trades.yml")):
        df = read_table(spark, "silver", table)
        results = run_contract(df, load_contract(REPO_ROOT / "contracts" / contract_file))
        all_results.extend(results)
        for r in results:
            print(f"  [{'PASS' if r.passed else 'FAIL'}] {r.dataset}: {r.check_name}")
    enforce(all_results)

    # -------------------------------------------------------------- 5. gold
    banner("5/8 GOLD: OHLCV bars + daily risk metrics")
    ohlcv = build_ohlcv_1m(
        read_table(spark, "silver", "ticks"), read_table(spark, "silver", "trades")
    )
    write_table(ohlcv, "gold", "ohlcv_1m", mode="overwrite", partition_by=["trade_date"])
    risk = build_symbol_risk_daily(read_table(spark, "gold", "ohlcv_1m"))
    write_table(risk, "gold", "symbol_risk_daily", mode="overwrite")
    print(f"  ohlcv_1m bars:      {ohlcv.count():,}")
    print(f"  symbol_risk_daily:  {risk.count():,}")

    # ------------------------------------------------------- 6. dq on gold
    banner("6/8 DATA CONTRACTS on gold")
    gold_results = run_contract(
        read_table(spark, "gold", "ohlcv_1m"),
        load_contract(REPO_ROOT / "contracts" / "gold_ohlcv_1m.yml"),
    )
    for r in gold_results:
        print(f"  [{'PASS' if r.passed else 'FAIL'}] {r.dataset}: {r.check_name}")
    enforce(gold_results)
    run_id = persist_results(all_results + gold_results)

    # --------------------------------------------------------- 7. warehouse
    banner("7/8 WAREHOUSE: load gold into DuckDB")
    counts = load_gold_tables()
    for table, n in counts.items():
        print(f"  gold.{table}: {n:,} rows loaded")

    # ------------------------------------------------------------ 8. report
    banner("8/8 REPORT (straight from the warehouse)")
    import duckdb

    con = duckdb.connect(str(workdir / "marketpulse.duckdb"))
    print("\n  Daily risk summary:")
    rows = con.execute(
        """
        SELECT symbol,
               ROUND(day_close, 2)                       AS close,
               ROUND(day_return_pct, 2)                  AS ret_pct,
               ROUND(realised_vol_annualised * 100, 1)   AS vol_pct,
               ROUND(max_drawdown * 100, 2)              AS max_dd_pct,
               total_volume                              AS volume,
               trade_count
        FROM gold.symbol_risk_daily ORDER BY symbol
        """
    ).fetchall()
    print(
        f"  {'symbol':<8}{'close':>10}{'ret%':>8}{'vol%':>8}{'maxDD%':>9}{'volume':>12}{'trades':>9}"
    )
    for r in rows:
        print(f"  {r[0]:<8}{r[1]:>10}{r[2]:>8}{r[3]:>8}{r[4]:>9}{r[5]:>12,}{r[6]:>9,}")
    dq_pass = con.execute(
        "SELECT COUNT(*) FILTER (passed), COUNT(*) FROM ops.dq_results WHERE run_id = ?",
        [run_id],
    ).fetchone()
    con.close()
    spark.stop()

    print(f"\n  DQ audit: {dq_pass[0]}/{dq_pass[1]} checks passed (run {run_id})")
    print(f"\nPipeline complete in {time.time() - t0:.1f}s")
    print(f"Lake:      {workdir / 'lake'}")
    print(f"Warehouse: {workdir / 'marketpulse.duckdb'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
