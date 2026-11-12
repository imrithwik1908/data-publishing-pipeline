"""Step 4 - PUBLISH or QUARANTINE.

Publish replaces one whole month (partition) inside a single DuckDB transaction:
either every statement lands, or none do. Quarantine never touches published tables.
"""

from __future__ import annotations

import json
from pathlib import Path

import duckdb

from dpp.checks import CheckResult
from dpp.contract import Contract


class InjectedFailure(RuntimeError):
    """Raised on purpose by tests to prove a half-finished publish leaves no trace."""


def publish(
    con: duckdb.DuckDBPyConnection,
    contract: Contract,
    batch_id: int,
    month: str,
    table: str,
    *,
    fail_after_delete: bool = False,
) -> tuple[int, int]:
    cols = ", ".join(f'"{c}"' for c in contract.column_names)
    con.execute("BEGIN TRANSACTION")
    try:
        con.execute("DELETE FROM fact_trips WHERE pickup_month = ?", [f"{month}-01"])
        if fail_after_delete:
            raise InjectedFailure("simulated crash in the middle of publish")
        con.execute(f"INSERT INTO fact_trips SELECT {cols}, pickup_month FROM {table} WHERE _reject_reason IS NULL")
        con.execute("DELETE FROM rejected_rows WHERE pickup_month = ?", [f"{month}-01"])
        con.execute(
            f"INSERT INTO rejected_rows SELECT {cols}, pickup_month, _reject_reason FROM {table} "
            "WHERE _reject_reason IS NOT NULL"
        )
        good, bad = con.execute(
            f"SELECT count(*) FILTER (WHERE _reject_reason IS NULL), count(_reject_reason) FROM {table}"
        ).fetchone()
        con.execute(
            "UPDATE batches SET status = 'superseded' WHERE month = ? AND status = 'published'",
            [month],
        )
        con.execute(
            """UPDATE batches SET status = 'published', row_count = ?, rejected_rows = ?,
                   finished_at = current_timestamp, message = 'published'
               WHERE batch_id = ?""",
            [good + bad, bad, batch_id],
        )
        con.execute("COMMIT")
    except BaseException:
        con.execute("ROLLBACK")
        raise
    con.execute(f"DROP TABLE IF EXISTS {table}")  # staging copy no longer needed
    return good, bad


def quarantine(
    con: duckdb.DuckDBPyConnection,
    batch_id: int,
    month: str,
    results: list[CheckResult],
    quarantine_dir: Path,
    row_count: int | None,
) -> Path:
    failing = [r for r in results if r.blocking]
    reason = "; ".join(f"{r.check_name}: {r.message}" for r in failing)
    con.execute(
        """UPDATE batches SET status = 'quarantined', row_count = ?, finished_at = current_timestamp,
               message = ? WHERE batch_id = ?""",
        [row_count, reason, batch_id],
    )
    out = quarantine_dir / f"batch_{batch_id}"
    out.mkdir(parents=True, exist_ok=True)
    path = out / "reasons.json"
    path.write_text(
        json.dumps(
            {
                "batch_id": batch_id,
                "month": month,
                "status": "quarantined",
                "failing_checks": [r.as_dict() for r in failing],
                "warnings": [r.as_dict() for r in results if r.severity == "WARN" and r.status == "fail"],
                "staging_table": f"stg_trips__{batch_id}",
            },
            indent=2,
        )
    )
    return path
