"""Each synthetic defect is caught (or, for the harmless one, only warned about)."""

from __future__ import annotations

import pyarrow.parquet as pq
import pytest

from dpp import synthetic

# defect -> the ERROR check that must fail (None = must publish with a warning)
EXPECTED = {
    "missing_column": "schema.missing_columns",
    "retyped_column": "schema.incompatible_types",
    "negative_fares": "rejected_row_rate",
    "wrong_month": "rejected_row_rate",
    "row_collapse": "row_count_band",
    "duplicated_rows": "duplicate_rows",
    "corrupt_file": "file_readable",
    "extra_column": None,
}


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
