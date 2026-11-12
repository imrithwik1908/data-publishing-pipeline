"""Each synthetic defect is caught (or, for the harmless one, only warned about)."""

from __future__ import annotations

import pyarrow.parquet as pq
import pytest

from dpp import synthetic

EXPECTED = synthetic.EXPECTED_CHECK


def _publish_baseline(files, run, months=("2024-10", "2024-11", "2024-12")):
    for m in months:
        assert run(files(m), m).status == "published"


def test_good_month_publishes_and_all_checks_pass(files, run):
    r = run(files("2024-10"), "2024-10")
    assert r.status == "published"
    assert not r.failing
    assert r.rows_rejected == 3  # the generator plants 3 dropoff-before-pickup rows


@pytest.mark.parametrize("defect", synthetic.DEFECTS)
def test_each_defect_behaves_as_specified(defect, files, run, tmp_path, fingerprint):
    _publish_baseline(files, run)
    before = fingerprint()
    good = pq.read_table(files("2025-01"))
    bad = synthetic.write_defect(good, defect, tmp_path / "bad" / f"{defect}.parquet")

    r = run(bad, "2025-01")

    assert synthetic.behaved_as_specified(defect, r)
    expected_check = EXPECTED[defect]
    if expected_check is None:
        assert r.status == "published"
        warned = {w.check_name for w in r.results if w.severity == "WARN" and w.status == "fail"}
        assert "schema.extra_columns" in warned
    else:
        assert r.status == "quarantined", f"{defect} was not quarantined"
        assert expected_check in {f.check_name for f in r.failing}
        assert r.reasons_path.exists()
        assert fingerprint() == before, "a quarantined batch changed the warehouse"


def test_numeric_type_drift_is_cast_with_warning(files, run, tmp_path):
    """Real TLC files switch passenger_count between int64 and double across years."""
    import pyarrow as pa

    t = pq.read_table(files("2024-10"))
    i = t.schema.get_field_index("passenger_count")
    t = t.set_column(i, pa.field("passenger_count", pa.float64()), t["passenger_count"].cast(pa.float64()))
    path = tmp_path / "drift.parquet"
    pq.write_table(t, path)
    r = run(path, "2024-10")
    assert r.status == "published"
    assert any(w.check_name == "schema.retyped_columns" and w.status == "fail" for w in r.results)


def test_freshness_blocks_gaps_unless_backfill(files, run):
    run(files("2024-10"), "2024-10")
    gap = run(files("2025-03"), "2025-03")
    assert gap.status == "quarantined"
    assert "freshness" in {f.check_name for f in gap.failing}
    assert run(files("2025-03"), "2025-03", mode="backfill").status == "published"


def test_same_file_under_another_month_is_rejected(files, run, tmp_path):
    path = files("2024-10")
    run(path, "2024-10")
    r = run(path, "2024-11")
    assert r.status == "quarantined"
    assert "duplicate_batch" in {f.check_name for f in r.failing}


def test_cbd_fee_column_is_optional_before_2025(files, run):
    """Dec 2024 lacks cbd_congestion_fee and Jan 2025 has it; both must publish."""
    assert run(files("2024-12"), "2024-12").status == "published"
    assert run(files("2025-01"), "2025-01").status == "published"


def test_missing_passenger_count_is_only_ok_for_flex_fares(files, run, tmp_path):
    """A non-flex trip with no passenger_count is a broken row, even if the null rate looks small."""
    import pyarrow as pa
    import pyarrow.compute as pc

    t = pq.read_table(files("2024-10"))
    card = pc.equal(t["payment_type"], 1)
    hit = pc.and_(card, pc.less(pa.array(range(t.num_rows)), 200))  # ~140 card rows lose passenger_count
    i = t.schema.get_field_index("passenger_count")
    t = t.set_column(i, "passenger_count", pc.if_else(hit, pa.scalar(None, pa.int64()), t["passenger_count"]))
    path = tmp_path / "pc.parquet"
    pq.write_table(t, path)
    r = run(path, "2024-10")
    rule = next(x for x in r.results if x.check_name == "row_rule.missing_fields_only_for_flex_fare")
    assert rule.status == "fail" and int(rule.observed) > 100
    assert r.status == "quarantined"  # > 0.5% of 5,000 rows
