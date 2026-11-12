"""Data contract: what a 'good' monthly file looks like. Loaded from YAML, validated by Pydantic."""

from __future__ import annotations

from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, Field

Severity = Literal["ERROR", "WARN"]


class Column(BaseModel):
    name: str
    type: str  # DuckDB type name, e.g. BIGINT, DOUBLE, TIMESTAMP
    required: bool = True
    max_null_rate: float | None = None
    aliases: list[str] = Field(default_factory=list)

    def matches(self, source_name: str) -> bool:
        names = {self.name.lower(), *(a.lower() for a in self.aliases)}
        return source_name.lower() in names


class RowRule(BaseModel):
    name: str
    sql: str


class SimpleCheck(BaseModel):
    severity: Severity


class RowCountBand(BaseModel):
    severity: Severity
    lookback_months: int = 3
    tolerance: float = 0.40


class DuplicateRows(BaseModel):
    severity: Severity
    max_rate: float
    key: list[str]


class Checks(BaseModel):
    extra_columns: SimpleCheck
    retyped_columns: SimpleCheck
    row_count_band: RowCountBand
    duplicate_rows: DuplicateRows
    freshness: SimpleCheck


class Contract(BaseModel):
    name: str
    version: int
    time_column: str
    columns: list[Column]
    row_rules: list[RowRule]
    max_rejected_row_rate: float
    checks: Checks

    @property
    def column_names(self) -> list[str]:
        return [c.name for c in self.columns]


def load_contract(path: str | Path) -> Contract:
    with open(path) as f:
        return Contract.model_validate(yaml.safe_load(f))
