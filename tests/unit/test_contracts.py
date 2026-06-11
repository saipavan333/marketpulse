"""Contract model + loader tests (no Spark needed)."""

from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import ValidationError

from marketpulse.quality.contracts import Contract, load_all_contracts, load_contract

CONTRACTS_DIR = Path(__file__).resolve().parents[2] / "contracts"


def test_all_repo_contracts_parse():
    contracts = load_all_contracts(CONTRACTS_DIR)
    assert len(contracts) >= 3
    names = {c.dataset for c in contracts}
    assert {"silver/ticks", "silver/trades", "gold/ohlcv_1m"} <= names


def test_contract_defaults():
    c = Contract(dataset="x/y")
    assert c.checks.min_rows is None
    assert c.columns == []


def test_invalid_severity_rejected(tmp_path):
    bad = tmp_path / "bad.yml"
    bad.write_text(
        """
dataset: a/b
columns:
  - { name: c1, severity: catastrophic }
"""
    )
    with pytest.raises(ValidationError):
        load_contract(bad)


def test_ticks_contract_content():
    c = load_contract(CONTRACTS_DIR / "silver_ticks.yml")
    by_name = {col.name: col for col in c.columns}
    assert by_name["bid"].not_null and by_name["bid"].min_value == 0.0001
    assert by_name["venue"].accepted_values == ["NYSE", "NASDAQ", "ARCA", "IEX"]
    assert c.checks.unique_key == ["symbol", "venue", "seq"]
