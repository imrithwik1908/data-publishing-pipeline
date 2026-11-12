"""Idempotency, atomicity, backfill order, and the run lock."""

from __future__ import annotations

import pytest

from dpp.lock import PipelineLocked, pipeline_lock
from dpp.publish import InjectedFailure


def test_rerun_same_file_is_skipped(files, run, fingerprint):
    p = files("2024-10")
    assert run(p, "2024-10").status == "published"
    fp = fingerprint()
    assert run(p, "2024-10").status == "skipped"
    assert fingerprint() == fp


def test_forced_rerun_five_times_gives_identical_tables(files, run, fingerprint):
    p = files("2024-10")
    run(p, "2024-10")
    first = (fingerprint("fact_trips"), fingerprint("rejected_rows"))
    for _ in range(5):
        assert run(p, "2024-10", force=True).status == "published"
        assert (fingerprint("fact_trips"), fingerprint("rejected_rows")) == first


def test_corrected_file_replaces_the_month_not_appends(files, run, fingerprint):
    run(files("2024-10", n=5000), "2024-10")
    run(files("2024-11", n=5000), "2024-11")
    r = run(files("2024-10", n=4000, seed=1), "2024-10")  # a corrected re-delivery
    assert r.status == "published"
    n, _ = fingerprint()
    assert n == (4000 - 3) + (5000 - 3)


def test_crash_during_publish_leaves_warehouse_unchanged(files, run, fingerprint, settings):
    run(files("2024-10"), "2024-10")
    before = (fingerprint("fact_trips"), fingerprint("rejected_rows"))
    with pytest.raises(InjectedFailure):
        run(files("2024-10", seed=7), "2024-10", fail_after_delete=True)
    assert (fingerprint("fact_trips"), fingerprint("rejected_rows")) == before

    import duckdb

    con = duckdb.connect(str(settings.warehouse))
    statuses = dict(con.execute("SELECT status, count(*) FROM batches GROUP BY 1").fetchall())
    con.close()
    assert statuses == {"published": 1, "failed": 1}


def test_backfill_order_does_not_change_final_state(tmp_path, files):
    from dpp.pipeline import run_file
    from dpp.settings import Settings
    from tests.conftest import CONTRACT

    months = ["2024-09", "2024-10", "2024-11", "2024-12"]
    paths = {m: files(m) for m in months}
    states = []
    for order in (months, list(reversed(months)), ["2024-11", "2024-09", "2024-12", "2024-10"]):
        s = Settings(data_dir=tmp_path / "-".join(order), contract_path=CONTRACT)
        for m in order:
            assert run_file(s, paths[m], m, mode="backfill").status == "published"
        import duckdb

        from dpp.db import table_fingerprint

        con = duckdb.connect(str(s.warehouse))
        states.append((table_fingerprint(con, "fact_trips"), table_fingerprint(con, "rejected_rows")))
        con.close()
    assert len(set(states)) == 1


def test_second_run_cannot_start_while_lock_held(files, run, settings):
    with pipeline_lock(settings.lock_file):
        with pytest.raises(PipelineLocked):
            run(files("2024-10"), "2024-10")
