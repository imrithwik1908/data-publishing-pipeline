"""The write-audit-publish flow, as separate steps.

    land_batch  -> 'landed'      (file copied, checksum + batch registered)
    audit_batch -> 'audited'     (staged with explicit types, every contract check stored)
                or 'quarantined' (an ERROR check failed; warehouse untouched)
    publish_batch -> 'published' (one transaction replaces the month)

Each step reads the batch's state from the `batches` table, so steps can run in one
process (`run_file`) or as separate Airflow tasks.
"""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path

import duckdb

from dpp import checks, db, land, publish, stage
from dpp.checks import AuditContext, CheckResult
from dpp.contract import Contract, load_contract
from dpp.lock import pipeline_lock
from dpp.settings import Settings


@dataclass
class RunResult:
    month: str
    status: str  # published | quarantined | skipped | audited | landed
    batch_id: int | None = None
    rows_published: int = 0
    rows_rejected: int = 0
    reasons_path: Path | None = None
    results: list[CheckResult] = field(default_factory=list)

    @property
    def failing(self) -> list[CheckResult]:
        return [r for r in self.results if r.blocking]

    @property
    def warnings(self) -> list[CheckResult]:
        return [r for r in self.results if r.severity == "WARN" and r.status == "fail"]


@contextmanager
def session(settings: Settings, *, locked: bool = True):
    """Open the warehouse (under the run lock) with tables and views ready."""
    contract = load_contract(settings.contract_path)

    def _open():
        con = db.connect(settings.warehouse)
        try:
            db.init_warehouse(con, contract, settings.zone_lookup)
            yield con, contract
        finally:
            con.close()

    if locked:
        with pipeline_lock(settings.lock_file):
            yield from _open()
    else:
        yield from _open()


def _batch(con: duckdb.DuckDBPyConnection, batch_id: int) -> dict:
    row = con.execute("SELECT * FROM batches WHERE batch_id = ?", [batch_id]).fetchone()
    if row is None:
        raise ValueError(f"no batch {batch_id}")
    return dict(zip([d[0] for d in con.description], row, strict=True))


def _load_results(con: duckdb.DuckDBPyConnection, batch_id: int) -> list[CheckResult]:
    rows = con.execute(
        "SELECT check_name, severity, status, observed, threshold, message FROM check_results WHERE batch_id = ?",
        [batch_id],
    ).fetchall()
    return [CheckResult(*r) for r in rows]


# ---------------- steps (take an open connection) ----------------


def _land(con, contract: Contract, settings: Settings, source: Path, month: str, mode: str, force: bool) -> RunResult:
    checksum = land.sha256_file(source)
    existing = land.already_published(con, month, checksum)
    if existing and not force:
        return RunResult(month, "skipped", batch_id=existing)
    landed = land.land_file(source, month, checksum, settings.landing_dir)
    batch_id = land.register_batch(con, month, landed, checksum, mode, contract.version)
    return RunResult(month, "landed", batch_id)


def _audit(con, contract: Contract, settings: Settings, batch_id: int) -> RunResult:
    b = _batch(con, batch_id)
    if b["status"] != "landed":
        raise publish.NotPublishable(f"batch {batch_id} is '{b['status']}', expected 'landed'")
    month, path = b["month"], Path(b["source_file"])

    # WRITE: an unreadable file is itself a failed check.
    try:
        src = stage.read_source_schema(path, contract)
        table = stage.load_staging(con, path, contract, src, batch_id, month)
    except (OSError, duckdb.Error, ValueError) as exc:
        results = [CheckResult("file_readable", "ERROR", "fail", type(exc).__name__, "valid parquet", str(exc)[:300])]
        checks.store_results(con, batch_id, results)
        reasons = publish.quarantine(con, batch_id, month, results, settings.quarantine_dir, None, None)
        return RunResult(month, "quarantined", batch_id, reasons_path=reasons, results=results)

    # AUDIT
    ctx = AuditContext(con, contract, table, month, batch_id, b["checksum"], b["mode"], src)
    results = [CheckResult("file_readable", "ERROR", "pass", message="parquet footer and schema read")]
    results += checks.run_checks(ctx)
    checks.store_results(con, batch_id, results)
    n, rejected = con.execute(f"SELECT count(*), count(_reject_reason) FROM {table}").fetchone()

    if any(r.blocking for r in results):
        reasons = publish.quarantine(con, batch_id, month, results, settings.quarantine_dir, n, rejected)
        return RunResult(month, "quarantined", batch_id, reasons_path=reasons, results=results)
    con.execute(
        "UPDATE batches SET status = 'audited', row_count = ?, rejected_rows = ? WHERE batch_id = ?",
        [n, rejected, batch_id],
    )
    return RunResult(month, "audited", batch_id, results=results)


def _publish(
    con, contract: Contract, settings: Settings, batch_id: int, *, allow: tuple[str, ...] = ("audited",),
    message: str = "published", fail_after_delete: bool = False,
) -> RunResult:  # fmt: skip
    b = _batch(con, batch_id)
    if b["status"] not in allow:
        # The guard that makes write-audit-publish real: nothing un-audited reaches the warehouse.
        raise publish.NotPublishable(f"batch {batch_id} is '{b['status']}'; only {allow} can be published")
    try:
        good, bad = publish.publish(
            con, contract, batch_id, b["month"], stage.staging_table(batch_id), settings.zone_lookup,
            message=message, fail_after_delete=fail_after_delete,
        )  # fmt: skip
    except Exception:
        con.execute(
            "UPDATE batches SET status = 'failed', finished_at = current_timestamp, "
            "message = 'publish transaction rolled back' WHERE batch_id = ?",
            [batch_id],
        )
        raise
    return RunResult(b["month"], "published", batch_id, good, bad, results=_load_results(con, batch_id))


# ---------------- public API (each call takes the run lock) ----------------


def land_batch(settings: Settings, source: Path, month: str, *, mode: str = "run", force: bool = False) -> RunResult:
    with session(settings) as (con, contract):
        return _land(con, contract, settings, source, month, mode, force)


def audit_batch(settings: Settings, batch_id: int) -> RunResult:
    with session(settings) as (con, contract):
        return _audit(con, contract, settings, batch_id)


def publish_batch(settings: Settings, batch_id: int) -> RunResult:
    with session(settings) as (con, contract):
        return _publish(con, contract, settings, batch_id)


def run_file(
    settings: Settings,
    source: Path,
    month: str,
    *,
    mode: str = "run",
    force: bool = False,
    fail_after_delete: bool = False,
) -> RunResult:
    """All steps for one file, holding the lock for the whole run."""
    with session(settings) as (con, contract):
        r = _land(con, contract, settings, source, month, mode, force)
        if r.status == "skipped":
            return r
        r = _audit(con, contract, settings, r.batch_id)
        if r.status == "quarantined":
            return r
        return _publish(con, contract, settings, r.batch_id, fail_after_delete=fail_after_delete)


def resolve(settings: Settings, batch_id: int, *, action: str, reason: str = "") -> RunResult:
    """A human's decision on a quarantined batch: 'discard' it or 'publish' it anyway (override)."""
    with session(settings) as (con, contract):
        b = _batch(con, batch_id)
        if b["status"] != "quarantined":
            raise publish.NotPublishable(f"batch {batch_id} is '{b['status']}', not quarantined")
        if action == "discard":
            con.execute(f"DROP TABLE IF EXISTS {stage.staging_table(batch_id)}")
            # Free the landed file too, unless another live batch points at the same content.
            (others,) = con.execute(
                "SELECT count(*) FROM batches WHERE source_file = ? AND batch_id <> ? AND status <> 'discarded'",
                [b["source_file"], batch_id],
            ).fetchone()
            if others == 0:
                Path(b["source_file"]).unlink(missing_ok=True)
            con.execute(
                "UPDATE batches SET status = 'discarded', message = message || ' | discarded: ' || ? "
                "WHERE batch_id = ?",
                [reason or "no reason given", batch_id],
            )
            return RunResult(b["month"], "discarded", batch_id, results=_load_results(con, batch_id))
        if action == "publish":
            if not reason:
                raise ValueError("an override needs a --reason; it is stored with the batch")
            if any(r.check_name == "file_readable" and r.status == "fail" for r in _load_results(con, batch_id)):
                raise publish.NotPublishable("an unreadable file cannot be published")
            return _publish(con, contract, settings, batch_id, allow=("quarantined",),
                            message=f"OVERRIDE: {reason}")  # fmt: skip
        raise ValueError(f"unknown action {action!r}")


def warehouse_assertions(settings: Settings) -> dict[str, int]:
    with session(settings, locked=False) as (con, _):
        return db.run_assertions(con)
