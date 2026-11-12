"""`make reproduce`: measure every number quoted in the README.

Writes results/metrics.json and results/RESULTS.md. Uses scratch warehouses under data/reproduce.
"""

from __future__ import annotations

import json
import platform
import shutil
import time
from datetime import UTC, datetime

import duckdb

from dpp import db, scenarios
from dpp.fetch import fetch_month, raw_path
from dpp.publish import InjectedFailure
from dpp.settings import REPO_ROOT, Settings

MONTHS = ["2024-11", "2024-12", "2025-01", "2025-02"]
RAW = REPO_ROOT / "data" / "raw"
WORK = REPO_ROOT / "data" / "reproduce"
CONTRACT = REPO_ROOT / "contracts" / "yellow_trips.yaml"


def fresh(name: str) -> Settings:
    d = WORK / name
    shutil.rmtree(d, ignore_errors=True)
    return Settings(data_dir=d, contract_path=CONTRACT)


def fingerprints(s: Settings) -> list:
    con = duckdb.connect(str(s.warehouse), read_only=True)
    try:
        return [db.table_fingerprint(con, t) for t in ("fact_trips", "rejected_rows")]
    finally:
        con.close()


def profile_real_data() -> list[dict]:
    out = []
    for m in MONTHS:
        p = str(raw_path(RAW, m))
        cols = {r[0] for r in duckdb.execute(f"DESCRIBE SELECT * FROM '{p}'").fetchall()}
        n, neg, neg_card, null_pc, null_pc_flex = duckdb.execute(f"""
            SELECT count(*), count(*) FILTER (WHERE fare_amount < 0),
                   count(*) FILTER (WHERE fare_amount < 0 AND payment_type = 1),
                   count(*) FILTER (WHERE passenger_count IS NULL),
                   count(*) FILTER (WHERE passenger_count IS NULL AND payment_type = 0)
            FROM '{p}'""").fetchone()  # fmt: skip
        out.append({
            "month": m, "rows": n, "negative_fare_pct": round(100 * neg / n, 2),
            "negative_fares_on_card": neg_card, "null_passenger_count_pct": round(100 * null_pc / n, 2),
            "null_passenger_count_that_are_flex_fare_pct": round(100 * null_pc_flex / max(null_pc, 1), 2),
            "has_cbd_congestion_fee": "cbd_congestion_fee" in cols,
        })  # fmt: skip
    return out


def main() -> None:
    started = time.perf_counter()
    for m in MONTHS:
        fetch_month(RAW, m)
    metrics: dict = {"generated_at": datetime.now(UTC).isoformat(timespec="seconds"),
                     "machine": f"{platform.system()} {platform.machine()}, Python {platform.python_version()}",
                     "duckdb": duckdb.__version__, "months": MONTHS}  # fmt: skip

    print("profiling real data ...")
    metrics["real_data_profile"] = profile_real_data()

    print("publishing real months in order ...")
    s = fresh("in_order")
    runs = scenarios.load_real_months(s, MONTHS, RAW)
    metrics["publish"] = [
        {"month": m, "status": r.status, "rows_published": r.rows_published, "rows_rejected": r.rows_rejected,
         "rejected_pct": round(100 * r.rows_rejected / (r.rows_published + r.rows_rejected), 4),
         "seconds": round(secs, 2), "rows_per_second": int((r.rows_published + r.rows_rejected) / secs)}
        for m, r, secs in runs
    ]  # fmt: skip
    in_order = fingerprints(s)

    print("injecting 8 defects into the latest real month ...")
    latest = MONTHS[-1]
    before = fingerprints(s)
    outcomes = scenarios.defect_matrix(s, raw_path(RAW, latest), latest)
    metrics["defects"] = [
        {"defect": o.defect, "expected_failing_check": o.expected, "status": o.result.status,
         "failing_checks": [f.check_name for f in o.result.failing], "behaved_as_specified": o.ok}
        for o in outcomes
    ]  # fmt: skip
    metrics["defects_handled_as_specified"] = f"{sum(o.ok for o in outcomes)}/{len(outcomes)}"
    quarantined = [o for o in outcomes if o.result.status == "quarantined"]
    # extra_column is allowed to publish; restore the clean month before comparing.
    scenarios.run_file(s, raw_path(RAW, latest), latest, force=True)
    metrics["quarantined_batches"] = len(quarantined)
    metrics["quarantined_batches_changed_warehouse"] = int(fingerprints(s) != before)

    print("re-running the latest month 5 times ...")
    base = fingerprints(s)
    same = 0
    for _ in range(5):
        scenarios.run_file(s, raw_path(RAW, latest), latest, force=True)
        same += fingerprints(s) == base
    metrics["rerun_identical_fingerprints"] = f"{same}/5"

    print("forcing a crash in the middle of publish ...")
    before = fingerprints(s)
    try:
        scenarios.run_file(s, raw_path(RAW, latest), latest, force=True, fail_after_delete=True)
    except InjectedFailure:
        pass
    metrics["partial_publishes_after_forced_crash"] = int(fingerprints(s) != before)

    print("backfilling the same months in reverse order ...")
    s2 = fresh("reverse_order")
    scenarios.load_real_months(s2, list(reversed(MONTHS)), RAW, mode="backfill")
    metrics["reverse_order_backfill_identical"] = fingerprints(s2) == in_order

    con = duckdb.connect(str(s.warehouse), read_only=True)
    metrics["warehouse_assertions"] = db.run_assertions(con)
    con.close()
    metrics["total_seconds"] = round(time.perf_counter() - started, 1)

    out = REPO_ROOT / "results"
    out.mkdir(exist_ok=True)
    (out / "metrics.json").write_text(json.dumps(metrics, indent=2, default=str))
    (out / "RESULTS.md").write_text(render(metrics))
    shutil.rmtree(WORK, ignore_errors=True)
    print((out / "RESULTS.md").read_text())


def render(m: dict) -> str:
    lines = [
        "# Results", "",
        f"Generated by `make reproduce` on {m['generated_at']} ({m['machine']}, DuckDB {m['duckdb']}).",
        "Every number in the README comes from this file.", "",
        "## Headline", "",
        f"- Injected defect types handled as specified: **{m['defects_handled_as_specified']}**",
        f"- Re-running a month 5 times gave identical table fingerprints: **{m['rerun_identical_fingerprints']}**",
        f"- Partial publishes after a forced crash mid-publish: **{m['partial_publishes_after_forced_crash']}**",
        f"- Quarantined batches that changed the warehouse: **{m['quarantined_batches_changed_warehouse']}**",
        f"- Reverse-order backfill produced the same warehouse: **{m['reverse_order_backfill_identical']}**",
        f"- Warehouse SQL assertions passing: **{sum(v == 0 for v in m['warehouse_assertions'].values())}"
        f"/{len(m['warehouse_assertions'])}**", "",
        "## Publishing real months", "",
        "| Month | Rows published | Rows rejected | Rejected % | Seconds | Rows/s |", "|---|---:|---:|---:|---:|---:|",
        *[f"| {p['month']} | {p['rows_published']:,} | {p['rows_rejected']:,} | {p['rejected_pct']}% | "
          f"{p['seconds']} | {p['rows_per_second']:,} |" for p in m["publish"]],
        "", "## Injected defects (on the latest real month)", "",
        "| Defect | Outcome | Failing checks | As specified |", "|---|---|---|---|",
        *[f"| {d['defect']} | {d['status']} | {', '.join(d['failing_checks']) or '-'} | "
          f"{'yes' if d['behaved_as_specified'] else 'NO'} |" for d in m["defects"]],
        "", "## What the real data looks like", "",
        "| Month | Rows | Negative fares | on card | Null passenger_count | of those, flex | cbd fee column |",
        "|---|---:|---:|---:|---:|---:|---|",
        *[f"| {p['month']} | {p['rows']:,} | {p['negative_fare_pct']}% | {p['negative_fares_on_card']} | "
          f"{p['null_passenger_count_pct']}% | {p['null_passenger_count_that_are_flex_fare_pct']}% | "
          f"{'yes' if p['has_cbd_congestion_fee'] else 'no'} |" for p in m["real_data_profile"]],
        "", f"Total runtime: {m['total_seconds']}s", "",
    ]  # fmt: skip
    return "\n".join(lines)


if __name__ == "__main__":
    main()
