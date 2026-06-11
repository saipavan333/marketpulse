"""Hourly medallion pipeline: bronze -> silver -> DQ -> gold -> DQ -> warehouse -> dbt.

Design decisions (interview talking points):
    - DQ gates are FIRST-CLASS TASKS between layers. Bad data fails the DAG
      *before* it can poison downstream tables — quality gates, not
      quality reports.
    - Tasks are idempotent: each run fully rebuilds its outputs for the
      window, so retries and backfills are safe (no double-counting).
    - dbt runs last, transforming warehouse gold into analyst marts and
      executing dbt tests as an extra safety net.
    - SLA + retries with exponential backoff; failures alert via the
      configured notifier (email/Slack in prod — see runbook).
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

DEFAULT_ARGS = {
    "owner": "data-platform",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "depends_on_past": False,
}

DBT_DIR = "/opt/airflow/dbt/marketpulse_dbt"
CONTRACTS = "/opt/airflow/contracts"


def _run_silver(**_):
    from marketpulse.batch import silver

    counts = silver.run()
    if counts.get("bronze", 0) == 0:
        raise ValueError("Bronze is empty — is the streaming job running?")
    return counts


def _run_gold(**_):
    from marketpulse.batch import gold

    return gold.run()


def _run_dq(contract_file: str, layer: str, table: str):
    def _inner(**_):
        from marketpulse.quality import load_contract, run_contract
        from marketpulse.quality.checks import enforce, persist_results
        from marketpulse.utils.spark import build_spark, read_table

        spark = build_spark(f"dq-{table}")
        results = run_contract(
            read_table(spark, layer, table), load_contract(f"{CONTRACTS}/{contract_file}")
        )
        persist_results(results)
        enforce(results)  # raises on error-severity failure -> task fails

    return _inner


def _load_warehouse(**_):
    from marketpulse.batch.warehouse import load_gold_tables

    return load_gold_tables()


with DAG(
    dag_id="marketpulse_medallion_hourly",
    description="bronze -> silver -> DQ -> gold -> DQ -> warehouse -> dbt",
    schedule="@hourly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=["marketpulse", "medallion", "hourly"],
) as dag:
    silver_task = PythonOperator(
        task_id="bronze_to_silver",
        python_callable=_run_silver,
        sla=timedelta(minutes=15),
    )

    dq_silver_ticks = PythonOperator(
        task_id="dq_silver_ticks",
        python_callable=_run_dq("silver_ticks.yml", "silver", "ticks"),
    )
    dq_silver_trades = PythonOperator(
        task_id="dq_silver_trades",
        python_callable=_run_dq("silver_trades.yml", "silver", "trades"),
    )

    gold_task = PythonOperator(
        task_id="silver_to_gold",
        python_callable=_run_gold,
        sla=timedelta(minutes=15),
    )

    dq_gold = PythonOperator(
        task_id="dq_gold_ohlcv",
        python_callable=_run_dq("gold_ohlcv_1m.yml", "gold", "ohlcv_1m"),
    )

    warehouse_task = PythonOperator(
        task_id="load_warehouse",
        python_callable=_load_warehouse,
    )

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=(f"cd {DBT_DIR} && dbt deps --profiles-dir . && dbt build --profiles-dir ."),
    )

    silver_task >> [dq_silver_ticks, dq_silver_trades] >> gold_task
    gold_task >> dq_gold >> warehouse_task >> dbt_build
