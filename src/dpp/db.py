"""Warehouse connection and table definitions (pipeline metadata + published fact table)."""

from __future__ import annotations

from pathlib import Path

import duckdb

from dpp.contract import Contract
from dpp.settings import SQL_DIR

# Batch lifecycle:
#   landed -> audited -> published -> superseded (a newer batch replaced the month)
#          \-> quarantined -> discarded (human threw it away) | published (human override)
#   failed = the publish transaction rolled back
STATUSES = ("landed", "audited", "quarantined", "published", "superseded", "discarded", "failed")


def connect(path: Path) -> duckdb.DuckDBPyConnection:
    path.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(path))


def init_warehouse(con: duckdb.DuckDBPyConnection, contract: Contract, zone_csv: Path) -> None:
    con.execute("CREATE SEQUENCE IF NOT EXISTS batch_seq START 1")
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS batches (
            batch_id      INTEGER PRIMARY KEY,
            month         VARCHAR NOT NULL,       -- 'YYYY-MM'
            source_file   VARCHAR NOT NULL,
            checksum      VARCHAR NOT NULL,       -- sha256 of the file bytes
            mode          VARCHAR NOT NULL,       -- 'run' or 'backfill'
            status        VARCHAR NOT NULL,       -- see STATUSES below
            contract_version INTEGER NOT NULL,
            row_count     BIGINT,
            rejected_rows BIGINT,
            created_at    TIMESTAMP NOT NULL DEFAULT current_timestamp,
            finished_at   TIMESTAMP,
            message       VARCHAR
        )
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS check_results (
            batch_id   INTEGER NOT NULL,
            check_name VARCHAR NOT NULL,
            severity   VARCHAR NOT NULL,
            status     VARCHAR NOT NULL,          -- pass | fail
            observed   VARCHAR,
            threshold  VARCHAR,
            message    VARCHAR
        )
        """
    )
    cols = ",\n".join(f'"{c.name}" {c.type}' for c in contract.columns)
    con.execute(f"CREATE TABLE IF NOT EXISTS fact_trips ({cols},\n pickup_month DATE NOT NULL)")
    con.execute(
        f"CREATE TABLE IF NOT EXISTS rejected_rows ({cols},\n"
        " pickup_month DATE NOT NULL, reject_reason VARCHAR NOT NULL)"
    )
    con.execute(
        """
        CREATE OR REPLACE VIEW rejected_batches AS
        SELECT b.batch_id, b.month, b.source_file, b.finished_at,
               string_agg(r.check_name || ': ' || r.message, '; ') AS reasons
        FROM batches b JOIN check_results r USING (batch_id)
        WHERE b.status = 'quarantined' AND r.status = 'fail' AND r.severity = 'ERROR'
        GROUP BY ALL
        """
    )
    if con.execute("SELECT count(*) FROM duckdb_tables() WHERE table_name = 'dim_zone'").fetchone()[0] == 0:
        run_models(con, zone_csv, only="dimensions")
    run_models(con, zone_csv, only="views")


def run_models(con: duckdb.DuckDBPyConnection, zone_csv: Path, only: str | None = None) -> None:
    """Execute sql/models/*.sql in name order (optionally only files containing `only`)."""
    for path in sorted((SQL_DIR / "models").glob("*.sql")):
        if only and only not in path.name:
            continue
        con.execute(path.read_text().replace("{zone_csv}", str(zone_csv)))


def run_assertions(con: duckdb.DuckDBPyConnection) -> dict[str, int]:
    """Run sql/assertions/*.sql. Each returns violating rows; returns {name: violation_count}."""
    out = {}
    for path in sorted((SQL_DIR / "assertions").glob("*.sql")):
        out[path.stem] = len(con.execute(path.read_text()).fetchall())
    return out


def table_fingerprint(con: duckdb.DuckDBPyConnection, table: str) -> tuple[int, int]:
    """Order-independent content fingerprint: (row count, sum of row hashes).

    Two tables with the same rows (in any physical order) get the same fingerprint.
    """
    n, h = con.execute(f"SELECT count(*), coalesce(sum(hash(t)::HUGEINT), 0) FROM {table} t").fetchone()
    return int(n), int(h)
