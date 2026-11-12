"""Step 1 - LAND: checksum the incoming file, copy it into landing/, register a batch."""

from __future__ import annotations

import hashlib
import os
import shutil
from pathlib import Path

import duckdb


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def already_published(con: duckdb.DuckDBPyConnection, month: str, checksum: str) -> int | None:
    row = con.execute(
        "SELECT batch_id FROM batches WHERE month = ? AND checksum = ? AND status = 'published'",
        [month, checksum],
    ).fetchone()
    return row[0] if row else None


def land_file(source: Path, month: str, checksum: str, landing_dir: Path) -> Path:
    """Copy the file into an immutable, content-addressed landing location."""
    dest = landing_dir / month / f"{checksum[:16]}.parquet"
    dest.parent.mkdir(parents=True, exist_ok=True)
    if not dest.exists():
        try:
            os.link(source, dest)  # same disk: no extra space used
        except OSError:
            shutil.copyfile(source, dest)  # different disk / filesystem without hardlinks
    return dest


def register_batch(
    con: duckdb.DuckDBPyConnection, month: str, source: Path, checksum: str, mode: str, contract_version: int
) -> int:
    (batch_id,) = con.execute(
        """
        INSERT INTO batches (batch_id, month, source_file, checksum, mode, status, contract_version)
        VALUES (nextval('batch_seq'), ?, ?, ?, ?, 'landed', ?)
        RETURNING batch_id
        """,
        [month, str(source), checksum, mode, contract_version],
    ).fetchone()
    return batch_id
