"""`make demo`: a fresh-clone walkthrough on real NYC TLC data.

1. Download 3 real months and publish them (2024-12 has no cbd_congestion_fee; 2025-01 adds it).
2. Re-deliver the latest month with each of the 8 defects injected, and show what happens.
3. Send the same good file again (skipped), run warehouse assertions, write the HTML report.

Uses its own warehouse under data/demo so it never touches your main one.
"""

from __future__ import annotations

import os
import shlex
import shutil
from pathlib import Path

from dpp import db, pipeline, scenarios
from dpp.fetch import raw_path
from dpp.pipeline import run_file
from dpp.report import build_report
from dpp.settings import REPO_ROOT, Settings

MONTHS = ["2024-12", "2025-01", "2025-02"]
GREEN, RED, YELLOW, DIM, END = "\033[32m", "\033[31m", "\033[33m", "\033[2m", "\033[0m"


def main() -> None:
    data = Path(os.environ.get("DPP_DEMO_DIR", REPO_ROOT / "data" / "demo"))
    raw = REPO_ROOT / "data" / "raw"  # downloads are shared with the main data dir
    shutil.rmtree(data, ignore_errors=True)
    s = Settings(data_dir=data, contract_path=REPO_ROOT / "contracts" / "yellow_trips.yaml")

    print("\n== 1. Publish three real months ==")
    for m, r, secs in scenarios.load_real_months(s, MONTHS, raw):
        print(f"{GREEN}[{m}] {r.status.upper():<11}{END} {r.rows_published:>10,} rows  "
              f"{r.rows_rejected:>6,} rejected rows  {secs:5.1f}s")  # fmt: skip

    latest = MONTHS[-1]
    print(f"\n== 2. Re-deliver {latest} with each defect injected ==")
    outcomes = scenarios.defect_matrix(s, raw_path(raw, latest), latest)
    for o in outcomes:
        mark = f"{GREEN}OK {END}" if o.ok else f"{RED}BAD{END}"
        if o.result.status == "quarantined":
            why = "; ".join(f.check_name for f in o.result.failing)
            print(f"{mark} {o.defect:<16} {RED}QUARANTINED{END} {DIM}{why}{END}")
        else:
            warns = ", ".join(w.check_name for w in o.result.warnings if w.check_name.startswith("schema"))
            print(f"{mark} {o.defect:<16} {YELLOW}{o.result.status.upper()}{END} {DIM}warnings: {warns}{END}")

    # Keep one quarantined batch for the report; discard the rest to save disk space.
    quarantined = [o for o in outcomes if o.result.status == "quarantined"]
    keep = next(o for o in quarantined if o.defect == "negative_fares")
    for o in quarantined:
        if o is not keep:
            pipeline.resolve(s, o.result.batch_id, action="discard", reason="demo cleanup")

    print("\n== 3. Same good file sent twice ==")
    r = run_file(s, raw_path(raw, latest), latest, force=True)  # restore the clean month after extra_column
    r = run_file(s, raw_path(raw, latest), latest)
    print(f"{YELLOW}[{latest}] {r.status.upper()}{END} identical file already published")

    print("\n== 4. Warehouse assertions ==")
    con = db.connect(s.warehouse)
    results = db.run_assertions(con)
    con.close()
    for name, n in results.items():
        print(f"{GREEN + 'PASS' if n == 0 else RED + 'FAIL'}{END} {name}")

    removed, before, after = pipeline.compact(s)
    print(f"\nCompacted: removed {removed} stale landed files, warehouse {before / 1e6:.0f} -> {after / 1e6:.0f} MB")

    report = build_report(s)
    caught = sum(o.ok for o in outcomes)
    print(f"\n{caught}/{len(outcomes)} injected defects handled as specified.")
    rel = lambda p: os.path.relpath(p, Path.cwd())  # noqa: E731
    print(f"Quarantined batch kept for inspection: {keep.result.batch_id} -> {rel(keep.result.reasons_path)}")
    print(f"Report: open {shlex.quote(rel(report))}")
    print(f"Try:    DPP_DATA_DIR={shlex.quote(rel(data))} uv run pipeline explain {keep.result.batch_id}\n")


if __name__ == "__main__":
    main()
