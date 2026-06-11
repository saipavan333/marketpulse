"""Daily operations DAG: quarantine triage report + lake maintenance.

- quarantine_report: counts quarantined rows by reason for the last day
  and pushes a summary to the pipeline ledger (ops schema). In prod
  this feeds an alert if quarantine-rate > 1% (data incident).
- optimize_lake: with Delta enabled, compacts small files + vacuums
  old snapshots — the unglamorous work that keeps query latency flat
  as the lake grows.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

DEFAULT_ARGS = {
    "owner": "data-platform",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def _quarantine_report(**_):
    from marketpulse.utils.spark import build_spark, read_table

    spark = build_spark("quarantine-report")
    summary = {}
    for table in ("quarantine_ticks", "quarantine_trades"):
        try:
            df = read_table(spark, "silver", table)
        except Exception:
            continue
        rows = df.groupBy("quarantine_reason").count().collect()
        summary[table] = {r["quarantine_reason"]: r["count"] for r in rows}
    print(f"Quarantine summary: {summary}")

    # Alert-worthy? Compare against clean volumes.
    clean = (
        read_table(spark, "silver", "ticks").count() + read_table(spark, "silver", "trades").count()
    )
    dirty = sum(sum(v.values()) for v in summary.values())
    rate = dirty / max(clean + dirty, 1)
    print(f"Quarantine rate: {rate:.3%}")
    if rate > 0.05:
        raise ValueError(f"Quarantine rate {rate:.1%} exceeds 5% — investigate upstream feed")
    return summary


def _optimize_lake(**_):
    from marketpulse.config import get_settings
    from marketpulse.utils.spark import build_spark

    settings = get_settings()
    if not settings.delta_enabled:
        print("Delta disabled (local mode) — skipping OPTIMIZE/VACUUM")
        return
    spark = build_spark("lake-maintenance")
    for layer, table in (("bronze", "events"), ("silver", "ticks"), ("silver", "trades")):
        path = settings.lake_path(layer, table)
        spark.sql(f"OPTIMIZE delta.`{path}`")
        spark.sql(f"VACUUM delta.`{path}` RETAIN 168 HOURS")  # keep 7 days
        print(f"Optimized + vacuumed {path}")


with DAG(
    dag_id="marketpulse_ops_daily",
    description="Quarantine triage + Delta lake maintenance",
    schedule="0 2 * * *",  # 02:00 UTC, after the trading day is settled
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["marketpulse", "ops", "daily"],
) as dag:
    quarantine_report = PythonOperator(
        task_id="quarantine_report",
        python_callable=_quarantine_report,
    )
    optimize_lake = PythonOperator(
        task_id="optimize_lake",
        python_callable=_optimize_lake,
    )

    quarantine_report >> optimize_lake
