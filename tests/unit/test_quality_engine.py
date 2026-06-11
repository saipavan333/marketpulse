"""DQ engine tests: every check type, severity semantics, enforcement."""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.spark

from marketpulse.quality.checks import ContractViolationError, enforce, run_contract  # noqa: E402
from marketpulse.quality.contracts import Contract  # noqa: E402


@pytest.fixture()
def df(spark):
    return spark.createDataFrame(
        [
            ("AAPL", 100.0, "BUY"),
            ("GS", 250.0, "SELL"),
            ("GS", None, "BUY"),
            ("MSFT", -5.0, "HOLD"),
        ],
        "symbol string, price double, side string",
    )


def _contract(**kwargs) -> Contract:
    base = {"dataset": "test/df"}
    base.update(kwargs)
    return Contract.model_validate(base)


def test_not_null_check_fails(df):
    c = _contract(columns=[{"name": "price", "not_null": True}])
    results = run_contract(df, c)
    not_null = next(r for r in results if r.check_name == "price.not_null")
    assert not not_null.passed
    assert "1 nulls" in not_null.observed


def test_range_check_fails_on_negative(df):
    c = _contract(columns=[{"name": "price", "min_value": 0.0001}])
    results = run_contract(df, c)
    rng = next(r for r in results if r.check_name == "price.range")
    assert not rng.passed


def test_accepted_values(df):
    c = _contract(columns=[{"name": "side", "accepted_values": ["BUY", "SELL"]}])
    results = run_contract(df, c)
    acc = next(r for r in results if r.check_name == "side.accepted_values")
    assert not acc.passed  # "HOLD" sneaks in


def test_unique_check(df):
    c = _contract(columns=[{"name": "symbol", "unique": True}])
    results = run_contract(df, c)
    uniq = next(r for r in results if r.check_name == "symbol.unique")
    assert not uniq.passed  # GS appears twice


def test_min_rows_passes(df):
    c = _contract(checks={"min_rows": 2})
    results = run_contract(df, c)
    assert all(r.passed for r in results if r.check_name == "min_rows")


def test_missing_column_reported(df):
    c = _contract(columns=[{"name": "ghost", "not_null": True}])
    results = run_contract(df, c)
    exists = next(r for r in results if r.check_name == "ghost.exists")
    assert not exists.passed


def test_enforce_raises_on_error_severity(df):
    c = _contract(columns=[{"name": "price", "not_null": True, "severity": "error"}])
    with pytest.raises(ContractViolationError):
        enforce(run_contract(df, c))


def test_enforce_tolerates_warn_severity(df):
    c = _contract(columns=[{"name": "price", "not_null": True, "severity": "warn"}])
    enforce(run_contract(df, c))  # must NOT raise


def test_clean_dataframe_passes_everything(spark):
    clean = spark.createDataFrame([("AAPL", 1.0), ("GS", 2.0)], "symbol string, price double")
    c = _contract(
        columns=[
            {"name": "symbol", "not_null": True, "unique": True},
            {"name": "price", "not_null": True, "min_value": 0},
        ],
        checks={"min_rows": 1, "unique_key": ["symbol"]},
    )
    results = run_contract(clean, c)
    assert all(r.passed for r in results)
