"""Where things live on disk. Everything sits under one data directory."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]


@dataclass(frozen=True)
class Settings:
    data_dir: Path
    contract_path: Path
    zone_lookup: Path = REPO_ROOT / "reference" / "taxi_zone_lookup.csv"

    @property
    def warehouse(self) -> Path:
        return self.data_dir / "warehouse.duckdb"

    @property
    def lock_file(self) -> Path:
        return self.data_dir / "warehouse.lock"

    @property
    def landing_dir(self) -> Path:
        return self.data_dir / "landing"

    @property
    def quarantine_dir(self) -> Path:
        return self.data_dir / "quarantine"

    @property
    def raw_dir(self) -> Path:
        return self.data_dir / "raw"

    @property
    def report_dir(self) -> Path:
        return self.data_dir / "report"


SQL_DIR = REPO_ROOT / "sql"


def default_settings() -> Settings:
    data_dir = Path(os.environ.get("DPP_DATA_DIR", REPO_ROOT / "data"))
    contract = Path(os.environ.get("DPP_CONTRACT", REPO_ROOT / "contracts" / "yellow_trips.yaml"))
    return Settings(data_dir=data_dir, contract_path=contract)
