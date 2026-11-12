"""Scenarios shared by `make demo` and `make reproduce`: real months plus injected defects."""

from __future__ import annotations

import tempfile
import time
from dataclasses import dataclass
from pathlib import Path

import pyarrow.parquet as pq

from dpp import synthetic
from dpp.fetch import fetch_month
from dpp.pipeline import RunResult, run_file  # noqa: F401  (re-exported for scripts)
from dpp.settings import Settings


@dataclass
class DefectOutcome:
    defect: str
    expected: str | None
    result: RunResult
    ok: bool


def load_real_months(settings: Settings, months: list[str], raw_dir: Path, *, mode: str = "run") -> list[tuple]:
    """Fetch + publish real months. Returns (month, result, seconds)."""
    out = []
    for m in months:
        path = fetch_month(raw_dir, m)
        t0 = time.perf_counter()
        r = run_file(settings, path, m, mode=mode)
        out.append((m, r, time.perf_counter() - t0))
    return out


def defect_matrix(settings: Settings, real_file: Path, month: str) -> list[DefectOutcome]:
    """Inject each of the 8 defects into a real file and re-deliver it as `month`."""
    table = pq.read_table(real_file)
    outcomes = []
    with tempfile.TemporaryDirectory() as tmp:
        for defect in synthetic.DEFECTS:
            bad = synthetic.write_defect(table, defect, Path(tmp) / f"{defect}.parquet", seed=42)
            r = run_file(settings, bad, month)
            outcomes.append(
                DefectOutcome(defect, synthetic.EXPECTED_CHECK[defect], r, synthetic.behaved_as_specified(defect, r))
            )
            bad.unlink()
    return outcomes
