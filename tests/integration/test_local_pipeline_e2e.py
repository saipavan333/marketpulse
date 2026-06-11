"""True end-to-end test: runs scripts/run_local_pipeline.py as a subprocess
and then audits the artifacts it produced — exactly what CI executes.

If this passes, the medallion flow (simulate -> bronze -> silver ->
contracts -> gold -> contracts -> warehouse) works on a clean machine.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.integration

REPO_ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture(scope="module")
def pipeline_run(tmp_path_factory):
    workdir = tmp_path_factory.mktemp("e2e")
    result = subprocess.run(
        [
            sys.executable,
            str(REPO_ROOT / "scripts" / "run_local_pipeline.py"),
            "--symbols", "AAPL,GS",
            "--minutes", "8",
            "--events-per-minute", "250",
            "--seed", "123",
            "--workdir", str(workdir),
        ],
        capture_output=True,
        text=True,
        timeout=900,
    )
    return workdir, result


def test_pipeline_exits_zero(pipeline_run):
    _workdir, result = pipeline_run
    assert result.returncode == 0, f"pipeline failed:\n{result.stdout}\n{result.stderr}"


def test_lake_layers_exist(pipeline_run):
    workdir, _ = pipeline_run
    for layer, table in (
        ("bronze", "events"),
        ("silver", "ticks"),
        ("silver", "trades"),
        ("gold", "ohlcv_1m"),
        ("gold", "symbol_risk_daily"),
    ):
        path = workdir / "lake" / layer / table
        assert path.exists(), f"missing lakehouse table {layer}/{table}"
        assert any(path.rglob("*.parquet")), f"no parquet files in {layer}/{table}"


def test_warehouse_contents(pipeline_run):
    workdir, _ = pipeline_run
    import duckdb

    con = duckdb.connect(str(workdir / "marketpulse.duckdb"), read_only=True)
    try:
        symbols = {r[0] for r in con.execute("SELECT DISTINCT symbol FROM gold.symbol_risk_daily").fetchall()}
        assert symbols == {"AAPL", "GS"}

        bars = con.execute("SELECT COUNT(*) FROM gold.ohlcv_1m").fetchone()[0]
        assert bars > 0

        # OHLC invariant holds all the way into the warehouse
        violations = con.execute(
            "SELECT COUNT(*) FROM gold.ohlcv_1m WHERE low > high OR open > high OR close < low"
        ).fetchone()[0]
        assert violations == 0

        # DQ audit trail was persisted and everything passed
        dq_total, dq_passed = con.execute(
            "SELECT COUNT(*), COUNT(*) FILTER (passed) FROM ops.dq_results"
        ).fetchone()
        assert dq_total > 0
        assert dq_passed == dq_total
    finally:
        con.close()


def test_pipeline_is_idempotent(pipeline_run):
    """Re-running with the same seed must produce identical gold output
    (overwrite semantics, deterministic generator)."""
    workdir, _ = pipeline_run
    import duckdb

    con = duckdb.connect(str(workdir / "marketpulse.duckdb"), read_only=True)
    first = con.execute(
        "SELECT symbol, total_volume, trade_count FROM gold.symbol_risk_daily ORDER BY symbol"
    ).fetchall()
    con.close()

    result = subprocess.run(
        [
            sys.executable,
            str(REPO_ROOT / "scripts" / "run_local_pipeline.py"),
            "--symbols", "AAPL,GS",
            "--minutes", "8",
            "--events-per-minute", "250",
            "--seed", "123",
            "--workdir", str(workdir),
        ],
        capture_output=True,
        text=True,
        timeout=900,
    )
    assert result.returncode == 0

    con = duckdb.connect(str(workdir / "marketpulse.duckdb"), read_only=True)
    second = con.execute(
        "SELECT symbol, total_volume, trade_count FROM gold.symbol_risk_daily ORDER BY symbol"
    ).fetchall()
    con.close()
    assert first == second
