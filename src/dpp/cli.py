"""Command-line interface: `pipeline run`, `pipeline ingest`, `pipeline status`."""

from __future__ import annotations

from pathlib import Path

import typer

from dpp import db
from dpp.pipeline import RunResult, run_file
from dpp.settings import default_settings

app = typer.Typer(add_completion=False, help="Write-audit-publish pipeline for monthly trip files.")


def _print(r: RunResult) -> None:
    if r.status == "published":
        typer.secho(
            f"[{r.month}] PUBLISHED batch {r.batch_id}: {r.rows_published:,} rows ({r.rows_rejected:,} rejected rows)",
            fg="green",
        )
        for w in r.results:
            if w.severity == "WARN" and w.status == "fail":
                typer.echo(f"    warn  {w.check_name}: {w.message}")
    elif r.status == "skipped":
        typer.secho(f"[{r.month}] SKIPPED: identical file already published (batch {r.batch_id})", fg="yellow")
    else:
        typer.secho(f"[{r.month}] QUARANTINED batch {r.batch_id}", fg="red")
        for f in r.failing:
            typer.echo(f"    FAIL  {f.check_name}: {f.message}  (observed {f.observed}; expected {f.threshold})")
        typer.echo(f"    reasons: {r.reasons_path}")


@app.command()
def ingest(
    file: Path = typer.Argument(..., exists=True, dir_okay=False),
    month: str = typer.Option(..., help="Month the file claims to contain, YYYY-MM"),
    backfill: bool = typer.Option(False, help="Allow months out of sequence"),
    force: bool = typer.Option(False, help="Re-publish even if this exact file was already published"),
) -> None:
    """Run one file through land -> stage -> audit -> publish/quarantine."""
    _print(run_file(default_settings(), file, month, mode="backfill" if backfill else "run", force=force))


@app.command()
def run(month: str, force: bool = typer.Option(False)) -> None:
    """Process data/raw/yellow_tripdata_<MONTH>.parquet."""
    s = default_settings()
    _print(run_file(s, s.raw_dir / f"yellow_tripdata_{month}.parquet", month, force=force))


@app.command()
def status() -> None:
    """Show batch history."""
    con = db.connect(default_settings().warehouse)
    con.sql("SELECT batch_id, month, status, row_count, rejected_rows, message FROM batches ORDER BY batch_id").show(
        max_width=200
    )


if __name__ == "__main__":
    app()
