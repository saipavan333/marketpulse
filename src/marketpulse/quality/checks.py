"""Contract execution engine: run declarative checks against Spark frames.

Every check returns a structured ``CheckResult``; results are written to
``ops.dq_results`` in the warehouse so quality is *observable over time*
(dashboards can chart pass-rate per dataset, per day).

Failure semantics:
    severity=error + failed  -> ``ContractViolationError`` raised -> task fails
    severity=warn  + failed  -> logged + recorded, pipeline continues
"""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from marketpulse.quality.contracts import Contract

logger = logging.getLogger(__name__)


class ContractViolationError(Exception):
    """Raised when one or more error-severity checks fail."""


@dataclass
class CheckResult:
    dataset: str
    check_name: str
    severity: str
    passed: bool
    observed: str
    threshold: str

    def as_row(self, run_id: str) -> dict:
        return {
            "run_id": run_id,
            "dataset": self.dataset,
            "check_name": self.check_name,
            "severity": self.severity,
            "passed": self.passed,
            "observed": self.observed,
            "threshold": self.threshold,
            "checked_at": datetime.now(timezone.utc).isoformat(),
        }


def run_contract(df: DataFrame, contract: Contract) -> list[CheckResult]:
    """Execute every check in a contract against a DataFrame."""
    results: list[CheckResult] = []
    ds = contract.dataset
    total = df.count()

    # ---------------------------------------------------------- dataset level
    checks = contract.checks
    if checks.min_rows is not None:
        results.append(
            CheckResult(ds, "min_rows", checks.severity, total >= checks.min_rows,
                        str(total), f">={checks.min_rows}")
        )
    if checks.unique_key:
        dupes = (
            df.groupBy(*checks.unique_key).count().where(F.col("count") > 1).count()
        )
        results.append(
            CheckResult(ds, f"unique_key({','.join(checks.unique_key)})",
                        checks.severity, dupes == 0, f"{dupes} duplicate keys", "0")
        )
    if checks.freshness_column and checks.freshness_max_age_minutes:
        max_ts = df.agg(F.max(checks.freshness_column)).collect()[0][0]
        if max_ts is None:
            results.append(CheckResult(ds, "freshness", checks.severity, False, "no data", ""))
        else:
            age_min = (datetime.now(timezone.utc) - max_ts.replace(tzinfo=timezone.utc)).total_seconds() / 60
            results.append(
                CheckResult(ds, "freshness", checks.severity,
                            age_min <= checks.freshness_max_age_minutes,
                            f"{age_min:.1f} min old",
                            f"<={checks.freshness_max_age_minutes} min")
            )

    # ----------------------------------------------------------- column level
    existing = set(df.columns)
    for col in contract.columns:
        if col.name not in existing:
            results.append(
                CheckResult(ds, f"{col.name}.exists", col.severity, False, "missing", "present")
            )
            continue
        if col.dtype:
            actual = dict(df.dtypes).get(col.name, "?")
            results.append(
                CheckResult(ds, f"{col.name}.dtype", col.severity, actual == col.dtype,
                            actual, col.dtype)
            )
        if col.not_null:
            nulls = df.where(F.col(col.name).isNull()).count()
            results.append(
                CheckResult(ds, f"{col.name}.not_null", col.severity, nulls == 0,
                            f"{nulls} nulls", "0")
            )
        if col.unique:
            distinct = df.select(col.name).distinct().count()
            non_null = df.where(F.col(col.name).isNotNull()).count()
            results.append(
                CheckResult(ds, f"{col.name}.unique", col.severity, distinct == non_null,
                            f"{non_null - distinct} duplicates", "0")
            )
        if col.accepted_values is not None:
            bad = df.where(
                F.col(col.name).isNotNull() & ~F.col(col.name).isin(col.accepted_values)
            ).count()
            results.append(
                CheckResult(ds, f"{col.name}.accepted_values", col.severity, bad == 0,
                            f"{bad} unexpected", str(col.accepted_values))
            )
        if col.min_value is not None or col.max_value is not None:
            cond = F.lit(False)
            if col.min_value is not None:
                cond = cond | (F.col(col.name) < col.min_value)
            if col.max_value is not None:
                cond = cond | (F.col(col.name) > col.max_value)
            out_of_range = df.where(F.col(col.name).isNotNull() & cond).count()
            results.append(
                CheckResult(ds, f"{col.name}.range", col.severity, out_of_range == 0,
                            f"{out_of_range} out of range",
                            f"[{col.min_value}, {col.max_value}]")
            )
    return results


def enforce(results: list[CheckResult]) -> None:
    """Log all results; raise if any error-severity check failed."""
    failed_errors = [r for r in results if not r.passed and r.severity == "error"]
    for r in results:
        level = logging.INFO if r.passed else logging.ERROR
        logger.log(level, "[DQ] %-45s %-5s observed=%s expected=%s",
                   f"{r.dataset}:{r.check_name}", "PASS" if r.passed else "FAIL",
                   r.observed, r.threshold)
    if failed_errors:
        names = ", ".join(f"{r.dataset}:{r.check_name}" for r in failed_errors)
        raise ContractViolationError(f"{len(failed_errors)} contract check(s) failed: {names}")


def persist_results(results: list[CheckResult], run_id: str | None = None) -> str:
    """Append results to the warehouse audit table (ops.dq_results)."""
    from marketpulse.config import get_settings

    run_id = run_id or str(uuid.uuid4())[:8]
    rows = [r.as_row(run_id) for r in results]
    settings = get_settings()
    url = settings.warehouse_url

    if url.startswith("duckdb"):
        import duckdb
        import pandas as pd

        path = url.replace("duckdb:///", "")
        con = duckdb.connect(path)
        con.execute("CREATE SCHEMA IF NOT EXISTS ops")
        con.execute(
            """CREATE TABLE IF NOT EXISTS ops.dq_results (
                   run_id TEXT, dataset TEXT, check_name TEXT, severity TEXT,
                   passed BOOLEAN, observed TEXT, threshold TEXT, checked_at TEXT)"""
        )
        results_df = pd.DataFrame(rows)
        con.register("results_df", results_df)
        con.execute("INSERT INTO ops.dq_results SELECT * FROM results_df")
        con.close()
    else:
        import pandas as pd
        from sqlalchemy import create_engine

        engine = create_engine(url)
        with engine.begin() as conn:
            pd.DataFrame(rows).to_sql(
                "dq_results", conn, schema="ops", if_exists="append", index=False
            )
    return run_id
