"""Star schema, analytic views, SQL assertions, step guards, human resolution, report, CLI."""

from __future__ import annotations

import duckdb
import pyarrow.parquet as pq
import pytest
from typer.testing import CliRunner

from dpp import pipeline, synthetic
from dpp.publish import NotPublishable
from dpp.report import build_report


def _q(settings, sql):
    con = duckdb.connect(str(settings.warehouse), read_only=True)
    try:
        return con.execute(sql).fetchall()
    finally:
        con.close()


@pytest.fixture
def three_months(files, run):
    for m in ("2024-10", "2024-11", "2024-12"):
        assert run(files(m), m).status == "published"


def test_dimensions_are_built(settings, three_months):
    assert _q(settings, "SELECT count(*) FROM dim_zone")[0][0] == 265
    assert _q(settings, "SELECT count(*) FROM dim_date")[0][0] == 31 + 30 + 31
    assert _q(settings, "SELECT payment_name FROM dim_payment_type WHERE payment_type = 4")[0][0] == "Dispute"


def test_all_sql_assertions_pass(settings, three_months):
    results = pipeline.warehouse_assertions(settings)
    assert results and all(v == 0 for v in results.values()), results


def test_assertion_catches_a_tampered_warehouse(settings, three_months):
    con = duckdb.connect(str(settings.warehouse))
    con.execute("UPDATE fact_trips SET pickup_month = DATE '2024-11-01' WHERE pickup_month = DATE '2024-10-01'")
    con.close()
    results = pipeline.warehouse_assertions(settings)
    assert results["fact_rows_inside_their_month"] > 0
    assert results["fact_months_match_published_batches"] > 0


def test_monthly_summary_lag_and_rank(settings, three_months):
    rows = _q(settings, "SELECT pickup_month, trips, prev_trips FROM v_monthly_summary ORDER BY 1")
    assert rows[0][2] is None
    assert rows[1][2] == rows[0][1]  # LAG gives the previous month
    ranks = _q(settings, "SELECT max(revenue_rank) <= 10 FROM v_top10_zones GROUP BY pickup_month")
    assert all(r[0] for r in ranks)


def test_running_total_restarts_each_month(settings, three_months):
    rows = _q(
        settings,
        "SELECT date_day, revenue, month_to_date_revenue FROM v_daily_running_revenue "
        "WHERE day(date_day) = 1 ORDER BY 1",
    )
    assert all(abs(r[1] - r[2]) < 0.01 for r in rows)


def test_cannot_publish_a_batch_that_was_not_audited(settings, files):
    r = pipeline.land_batch(settings, files("2024-10"), "2024-10")
    with pytest.raises(NotPublishable):
        pipeline.publish_batch(settings, r.batch_id)


def test_steps_run_separately_like_airflow_tasks(settings, files):
    landed = pipeline.land_batch(settings, files("2024-10"), "2024-10")
    audited = pipeline.audit_batch(settings, landed.batch_id)
    assert audited.status == "audited"
    assert pipeline.publish_batch(settings, landed.batch_id).status == "published"


def test_discard_quarantined_batch(settings, files, run, tmp_path, fingerprint):
    run(files("2024-10"), "2024-10")
    before = fingerprint()
    bad = synthetic.write_defect(pq.read_table(files("2024-11")), "row_collapse", tmp_path / "b.parquet")
    q = run(bad, "2024-11")
    assert q.status == "quarantined"
    assert pipeline.resolve(settings, q.batch_id, action="discard").status == "discarded"
    assert fingerprint() == before
    assert not _q(settings, f"SELECT 1 FROM duckdb_tables() WHERE table_name = 'stg_trips__{q.batch_id}'")


def test_override_needs_a_reason_and_is_recorded(settings, files, run, tmp_path):
    run(files("2024-10"), "2024-10")
    bad = synthetic.write_defect(pq.read_table(files("2024-11")), "row_collapse", tmp_path / "b.parquet")
    q = run(bad, "2024-11")
    with pytest.raises(ValueError):
        pipeline.resolve(settings, q.batch_id, action="publish")
    r = pipeline.resolve(settings, q.batch_id, action="publish", reason="TLC confirmed a partial outage month")
    assert r.status == "published"
    msg = _q(settings, f"SELECT message FROM batches WHERE batch_id = {q.batch_id}")[0][0]
    assert msg.startswith("OVERRIDE: TLC confirmed")


def test_unreadable_file_can_never_be_overridden(settings, files, run, tmp_path):
    bad = synthetic.write_defect(pq.read_table(files("2024-10")), "corrupt_file", tmp_path / "c.parquet")
    q = run(bad, "2024-10")
    with pytest.raises(NotPublishable):
        pipeline.resolve(settings, q.batch_id, action="publish", reason="please")


def test_report_lists_quarantined_batch(settings, files, run, tmp_path):
    run(files("2024-10"), "2024-10")
    bad = synthetic.write_defect(pq.read_table(files("2024-11")), "missing_column", tmp_path / "m.parquet")
    q = run(bad, "2024-11")
    html = build_report(settings).read_text()
    assert f"Batch {q.batch_id}" in html and "schema.missing_columns" in html


def test_cli_end_to_end(settings, files, monkeypatch):
    from dpp.cli import app

    monkeypatch.setenv("DPP_DATA_DIR", str(settings.data_dir))
    runner = CliRunner()
    good = files("2024-10")
    r = runner.invoke(app, ["ingest", str(good), "--month", "2024-10"])
    assert r.exit_code == 0 and "PUBLISHED" in r.output
    r = runner.invoke(app, ["ingest", str(good), "--month", "2024-10"])
    assert "SKIPPED" in r.output
    assert runner.invoke(app, ["verify"]).exit_code == 0
    assert runner.invoke(app, ["status"]).exit_code == 0
    assert runner.invoke(app, ["explain", "1", "--json"]).exit_code == 0
    assert "report:" in runner.invoke(app, ["report"]).output


def test_discard_frees_the_landed_file(settings, files, run, tmp_path):
    from pathlib import Path

    run(files("2024-10"), "2024-10")
    bad = synthetic.write_defect(pq.read_table(files("2024-11")), "row_collapse", tmp_path / "b.parquet")
    q = run(bad, "2024-11")
    landed = Path(_q(settings, f"SELECT source_file FROM batches WHERE batch_id = {q.batch_id}")[0][0])
    assert landed.exists()
    pipeline.resolve(settings, q.batch_id, action="discard")
    assert not landed.exists()
