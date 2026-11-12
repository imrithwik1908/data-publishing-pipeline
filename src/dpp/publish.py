"""Step 4 - PUBLISH or QUARANTINE, plus the human decisions on a quarantined batch.

Publish replaces one whole month (partition) inside a single DuckDB transaction:
either every statement lands, or none do. Quarantine never touches published tables.
"""

from __future__ import annotations

import json
from pathlib import Path

import duckdb

from dpp import db
from dpp.checks import CheckResult
from dpp.contract import Contract


class InjectedFailure(RuntimeError):
    """Raised on purpose by tests to prove a half-finished publish leaves no trace."""


class NotPublishable(RuntimeError):
    pass


def publish(
    con: duckdb.DuckDBPyConnection,
    contract: Contract,
    batch_id: int,
    month: str,
    table: str,
    zone_csv: Path,
    *,
    message: str = "published",
    fail_after_delete: bool = False,
) -> tuple[int, int]:
    cols = ", ".join(f'"{c}"' for c in contract.column_names)
    month_start = f"{month}-01"
    con.execute("BEGIN TRANSACTION")
    try:
        con.execute("DELETE FROM fact_trips WHERE pickup_month = ?", [month_start])
        if fail_after_delete:
            raise InjectedFailure("simulated crash in the middle of publish")
        con.execute(f"INSERT INTO fact_trips SELECT {cols}, pickup_month FROM {table} WHERE _reject_reason IS NULL")
        con.execute("DELETE FROM rejected_rows WHERE pickup_month = ?", [month_start])
        con.execute(
            f"INSERT INTO rejected_rows SELECT {cols}, pickup_month, _reject_reason FROM {table} "
            "WHERE _reject_reason IS NOT NULL"
        )
        db.run_models(con, zone_csv, only="dimensions")  # refresh dims in the same transaction
        good, bad = con.execute(
            f"SELECT count(*) FILTER (WHERE _reject_reason IS NULL), count(_reject_reason) FROM {table}"
        ).fetchone()
        con.execute(
            "UPDATE batches SET status = 'superseded' WHERE month = ? AND status = 'published'",
            [month],
        )
        con.execute(
            """UPDATE batches SET status = 'published', row_count = ?, rejected_rows = ?,
                   finished_at = current_timestamp, message = ?
               WHERE batch_id = ?""",
            [good + bad, bad, message, batch_id],
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
    rejected_rows: int | None,
) -> Path:
    failing = [r for r in results if r.blocking]
    reason = "; ".join(f"{r.check_name}: {r.message}" for r in failing)
    con.execute(
        """UPDATE batches SET status = 'quarantined', row_count = ?, rejected_rows = ?,
               finished_at = current_timestamp, message = ? WHERE batch_id = ?""",
        [row_count, rejected_rows, reason, batch_id],
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
                "next_steps": [
                    f"inspect:  SELECT * FROM stg_trips__{batch_id} LIMIT 20",
                    f"discard:  pipeline resolve {batch_id} --discard",
                    f'override: pipeline resolve {batch_id} --publish --reason "why this is OK"',
                ],
            },
            indent=2,
        )
    )
    return path
