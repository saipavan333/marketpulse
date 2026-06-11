"""Data contracts: YAML-defined expectations for every lakehouse table.

A contract declares what a dataset *promises* its consumers:
    - required columns and types
    - row-level checks (not_null, unique, accepted_values, range)
    - dataset-level checks (min_rows, freshness)

Contracts live in ``contracts/*.yml`` next to the code, are validated by
this module at load time, and are executed by ``quality.checks`` inside
the pipeline (Airflow runs them as first-class tasks that can fail a DAG).

Why hand-rolled instead of Great Expectations? (interview talking point)
    GE is powerful but heavyweight; a contracts engine is ~150 lines,
    dependency-free, easy to reason about in CI, and demonstrates that
    you understand *what* DQ tools do under the hood. Swapping in GE or
    Soda later is an isolated change — the contracts stay declarative.
"""

from __future__ import annotations

from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, Field

Severity = Literal["error", "warn"]


class ColumnSpec(BaseModel):
    name: str
    dtype: str | None = None
    not_null: bool = False
    unique: bool = False
    accepted_values: list[str] | None = None
    min_value: float | None = None
    max_value: float | None = None
    severity: Severity = "error"


class DatasetChecks(BaseModel):
    min_rows: int | None = None
    freshness_column: str | None = None
    freshness_max_age_minutes: int | None = None
    unique_key: list[str] | None = None
    severity: Severity = "error"


class Contract(BaseModel):
    """A complete data contract for one dataset."""

    dataset: str = Field(description="layer/table, e.g. silver/ticks")
    owner: str = "data-platform"
    description: str = ""
    columns: list[ColumnSpec] = Field(default_factory=list)
    checks: DatasetChecks = Field(default_factory=DatasetChecks)


def load_contract(path: str | Path) -> Contract:
    """Parse and validate a contract YAML file."""
    raw = yaml.safe_load(Path(path).read_text())
    return Contract.model_validate(raw)


def load_all_contracts(directory: str | Path = "contracts") -> list[Contract]:
    """Load every ``*.yml`` contract in a directory."""
    return [load_contract(p) for p in sorted(Path(directory).glob("*.yml"))]
