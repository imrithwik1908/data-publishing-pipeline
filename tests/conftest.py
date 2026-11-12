from __future__ import annotations

from pathlib import Path

import pytest

from dpp import db, synthetic
from dpp.contract import load_contract
from dpp.pipeline import run_file
from dpp.settings import REPO_ROOT, Settings

CONTRACT = REPO_ROOT / "contracts" / "yellow_trips.yaml"


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    return Settings(data_dir=tmp_path / "data", contract_path=CONTRACT)


@pytest.fixture
def files(tmp_path: Path):
    """Factory: good synthetic file for a month."""

    def make(month: str, n: int = 5000, seed: int = 0) -> Path:
        return synthetic.write_good(month, tmp_path / "in" / f"{month}_{seed}_{n}.parquet", n=n, seed=seed)

    return make


@pytest.fixture
def run(settings):
    def _run(path: Path, month: str, **kw):
        return run_file(settings, path, month, **kw)

    return _run


@pytest.fixture
def fingerprint(settings):
    def _fp(table: str = "fact_trips"):
        con = db.connect(settings.warehouse)
        try:
            return db.table_fingerprint(con, table)
        finally:
            con.close()

    return _fp


@pytest.fixture
def contract():
    return load_contract(CONTRACT)
