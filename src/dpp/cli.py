"""Command-line interface.

pipeline fetch 2024-11 2025-01           download months into data/raw
pipeline run 2025-01                      fetch if needed, then land -> audit -> publish
pipeline ingest FILE --month 2025-01      process any file you were handed
pipeline backfill --from 2024-07 --to 2025-06
pipeline status | explain BATCH | resolve BATCH --discard/--publish | report | verify
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import typer

from dpp import months, pipeline
from dpp.fetch import FetchError, fetch_month
from dpp.lock import PipelineLocked
from dpp.pipeline import RunResult
from dpp.publish import NotPublishable
from dpp.settings import default_settings

app = typer.Typer(add_completion=False, no_args_is_help=True,
                  help="Write-audit-publish pipeline for monthly trip files.")  # fmt: skip


def _print(r: RunResult) -> None:
    if r.status == "published":
        typer.secho(f"[{r.month}] PUBLISHED  batch {r.batch_id}: {r.rows_published:,} rows "
                    f"({r.rows_rejected:,} rejected rows)", fg="green")  # fmt: skip
        for w in r.warnings:
            typer.echo(f"    warn  {w.check_name}: {w.message}")
    elif r.status == "skipped":
        typer.secho(f"[{r.month}] SKIPPED    identical file already published (batch {r.batch_id})", fg="yellow")
    elif r.status == "discarded":
        typer.secho(f"[{r.month}] DISCARDED  batch {r.batch_id}", fg="yellow")
    else:
        typer.secho(f"[{r.month}] QUARANTINED batch {r.batch_id}", fg="red")
        for f in r.failing:
            typer.echo(f"    FAIL  {f.check_name}: {f.message}")
            typer.echo(f"          observed {f.observed}; expected {f.threshold}")
        typer.echo(f"    reasons: {r.reasons_path}")


def _guard(fn):
    """Turn expected operational errors into a clean message and exit code 2."""
    try:
        return fn()
    except (PipelineLocked, FetchError, NotPublishable, ValueError) as exc:
        typer.secho(f"error: {exc}", fg="red", err=True)
        raise typer.Exit(2) from exc


@app.command()
def fetch(month: list[str] = typer.Argument(..., help="One or more YYYY-MM months")) -> None:
    """Download TLC monthly files into data/raw (skips files already there)."""
    s = default_settings()
    for m in month:
        path = _guard(lambda m=m: fetch_month(s.raw_dir, m))
        typer.echo(f"{m}: {path} ({path.stat().st_size / 1e6:.0f} MB)")


@app.command()
def run(
    month: str,
    force: bool = typer.Option(False, help="Re-publish even if this exact file was already published"),
    backfill: bool = typer.Option(False, help="Allow a month out of sequence"),
) -> None:
    """Fetch (if needed) and process one month."""
    s = default_settings()
    path = _guard(lambda: fetch_month(s.raw_dir, month))
    r = _guard(lambda: pipeline.run_file(s, path, month, mode="backfill" if backfill else "run", force=force))
    _print(r)
    if r.status == "quarantined":
        raise typer.Exit(1)


@app.command()
def ingest(
    file: Path = typer.Argument(..., exists=True, dir_okay=False),
    month: str = typer.Option(..., help="Month the file claims to contain, YYYY-MM"),
    backfill: bool = typer.Option(False, help="Allow months out of sequence"),
    force: bool = typer.Option(False, help="Re-publish even if this exact file was already published"),
) -> None:
    """Process any local file: land -> stage -> audit -> publish or quarantine."""
    s = default_settings()
    r = _guard(lambda: pipeline.run_file(s, file, month, mode="backfill" if backfill else "run", force=force))
    _print(r)
    if r.status == "quarantined":
        raise typer.Exit(1)


@app.command()
def backfill(
    start: str = typer.Option(..., "--from", help="First month, YYYY-MM"),
    end: str = typer.Option(..., "--to", help="Last month, YYYY-MM"),
    force: bool = typer.Option(False),
) -> None:
    """Process a range of months. A quarantined month does not stop the others."""
    s = default_settings()
    counts: dict[str, int] = {}
    for m in months.month_range(start, end):
        try:
            path = fetch_month(s.raw_dir, m)
        except FetchError as exc:
            typer.secho(f"[{m}] NOT FETCHED {exc}", fg="yellow")
            counts["not fetched"] = counts.get("not fetched", 0) + 1
            continue
        r = _guard(lambda m=m, path=path: pipeline.run_file(s, path, m, mode="backfill", force=force))
        _print(r)
        counts[r.status] = counts.get(r.status, 0) + 1
    typer.echo("summary: " + ", ".join(f"{v} {k}" for k, v in counts.items()))


@app.command()
def status(limit: int = 30) -> None:
    """Batch history, newest first."""
    with pipeline.session(default_settings(), locked=False) as (con, _):
        con.sql(
            f"SELECT batch_id, month, status, mode, row_count, rejected_rows, errors, warnings, "
            f"left(message, 70) AS message FROM v_batch_quality ORDER BY batch_id DESC LIMIT {int(limit)}"
        ).show(max_width=200)


@app.command()
def explain(batch_id: int, as_json: bool = typer.Option(False, "--json")) -> None:
    """Every check result for one batch."""
    with pipeline.session(default_settings(), locked=False) as (con, _):
        rel = con.sql(
            f"SELECT check_name, severity, status, observed, threshold, message FROM check_results "
            f"WHERE batch_id = {int(batch_id)} ORDER BY status DESC, severity, check_name"
        )
        if as_json:
            cols = rel.columns
            json.dump([dict(zip(cols, r, strict=True)) for r in rel.fetchall()], sys.stdout, indent=2)
            typer.echo()
        else:
            rel.show(max_width=200, max_rows=100)


@app.command()
def resolve(
    batch_id: int,
    discard: bool = typer.Option(False, "--discard", help="Throw the batch away"),
    publish_: bool = typer.Option(False, "--publish", help="Publish despite failed checks (override)"),
    reason: str = typer.Option("", help="Why; stored on the batch. Required for --publish."),
) -> None:
    """Record a human decision on a quarantined batch."""
    if discard == publish_:
        typer.secho("choose exactly one of --discard or --publish", fg="red", err=True)
        raise typer.Exit(2)
    s = default_settings()
    action = "discard" if discard else "publish"
    _print(_guard(lambda: pipeline.resolve(s, batch_id, action=action, reason=reason)))


@app.command()
def compact() -> None:
    """Reclaim disk: prune superseded landed files and rewrite the warehouse file."""
    removed, before, after = _guard(lambda: pipeline.compact(default_settings()))
    typer.echo(f"removed {removed} stale landed files; warehouse {before / 1e6:.0f} MB -> {after / 1e6:.0f} MB")


@app.command()
def report() -> None:
    """Write the static HTML run-history report."""
    from dpp.report import build_report

    path = build_report(default_settings())
    typer.echo(f"report: {path}")


@app.command()
def verify() -> None:
    """Run the SQL assertions in sql/assertions against the warehouse."""
    results = pipeline.warehouse_assertions(default_settings())
    for name, n in results.items():
        typer.secho(f"{'PASS' if n == 0 else 'FAIL'}  {name}" + (f" ({n} violations)" if n else ""),
                    fg="green" if n == 0 else "red")  # fmt: skip
    if any(results.values()):
        raise typer.Exit(1)


if __name__ == "__main__":
    app()
