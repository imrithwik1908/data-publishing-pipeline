"""The whole write-audit-publish flow for one monthly file."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from dpp import checks, db, land, publish, stage
from dpp.checks import AuditContext, CheckResult
from dpp.contract import load_contract
from dpp.lock import pipeline_lock
from dpp.settings import Settings


@dataclass
class RunResult:
    month: str
    status: str  # published | quarantined | skipped
    batch_id: int | None = None
    rows_published: int = 0
    rows_rejected: int = 0
    reasons_path: Path | None = None
    results: list[CheckResult] = field(default_factory=list)

    @property
    def failing(self) -> list[CheckResult]:
        return [r for r in self.results if r.blocking]


def run_file(
    settings: Settings,
    source: Path,
    month: str,
    *,
    mode: str = "run",
    force: bool = False,
    fail_after_delete: bool = False,
) -> RunResult:
    contract = load_contract(settings.contract_path)
    with pipeline_lock(settings.lock_file):
        con = db.connect(settings.warehouse)
        try:
            db.init_warehouse(con, contract)

            # 1. LAND
            checksum = land.sha256_file(source)
            existing = land.already_published(con, month, checksum)
            if existing and not force:
                return RunResult(month, "skipped", batch_id=existing)
            landed = land.land_file(source, month, checksum, settings.landing_dir)
            batch_id = land.register_batch(con, month, landed, checksum, mode)

            # 2. WRITE (stage). An unreadable file is itself a failed check.
            try:
                src = stage.read_source_schema(landed, contract)
                table = stage.load_staging(con, landed, contract, src, batch_id, month)
            except Exception as exc:  # noqa: BLE001 - any read error means "unreadable file"
                results = [
                    CheckResult(
                        "file_readable",
                        "ERROR",
                        "fail",
                        observed=type(exc).__name__,
                        threshold="valid parquet",
                        message=str(exc)[:300],
                    )
                ]
                checks.store_results(con, batch_id, results)
                path = publish.quarantine(con, batch_id, month, results, settings.quarantine_dir, None)
                return RunResult(month, "quarantined", batch_id, reasons_path=path, results=results)

            # 3. AUDIT
            ctx = AuditContext(con, contract, table, month, batch_id, checksum, mode, src)
            results = [CheckResult("file_readable", "ERROR", "pass", message="parquet footer and schema read")]
            results += checks.run_checks(ctx)
            checks.store_results(con, batch_id, results)

            # 4. DECIDE
            if any(r.blocking for r in results):
                (n,) = con.execute(f"SELECT count(*) FROM {table}").fetchone()
                path = publish.quarantine(con, batch_id, month, results, settings.quarantine_dir, n)
                return RunResult(month, "quarantined", batch_id, reasons_path=path, results=results)

            try:
                good, bad = publish.publish(con, contract, batch_id, month, table, fail_after_delete=fail_after_delete)
            except Exception:
                con.execute(
                    "UPDATE batches SET status = 'failed', finished_at = current_timestamp, "
                    "message = 'publish transaction rolled back' WHERE batch_id = ?",
                    [batch_id],
                )
                raise
            return RunResult(month, "published", batch_id, good, bad, results=results)
        finally:
            con.close()
