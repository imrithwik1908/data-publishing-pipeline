"""Download monthly TLC files into data/raw (the 'landing zone' an upstream team would drop into)."""

from __future__ import annotations

import urllib.error
import urllib.request
from pathlib import Path

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{month}.parquet"
MAX_TOTAL_BYTES = 1_100_000_000  # keep the whole raw folder near the 1 GB budget


class FetchError(RuntimeError):
    pass


def raw_path(raw_dir: Path, month: str) -> Path:
    return raw_dir / f"yellow_tripdata_{month}.parquet"


def fetch_month(raw_dir: Path, month: str, *, timeout: int = 120) -> Path:
    """Download one month if missing. Writes to a .part file first so a crash never leaves half a file."""
    dest = raw_path(raw_dir, month)
    if dest.exists():
        return dest
    raw_dir.mkdir(parents=True, exist_ok=True)
    used = sum(p.stat().st_size for p in raw_dir.glob("*.parquet"))
    if used > MAX_TOTAL_BYTES:
        raise FetchError(f"raw folder already holds {used / 1e9:.2f} GB; refusing to download more")
    tmp = dest.with_suffix(".part")
    req = urllib.request.Request(BASE_URL.format(month=month), headers={"User-Agent": "dpp-pipeline"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp, open(tmp, "wb") as out:
            while chunk := resp.read(1 << 20):
                out.write(chunk)
    except urllib.error.HTTPError as exc:
        tmp.unlink(missing_ok=True)
        if exc.code in (403, 404):
            raise FetchError(f"{month}: not published by TLC yet (HTTP {exc.code})") from exc
        raise FetchError(f"{month}: download failed (HTTP {exc.code})") from exc
    except OSError as exc:
        tmp.unlink(missing_ok=True)
        raise FetchError(f"{month}: download failed ({exc})") from exc
    tmp.rename(dest)  # atomic on the same filesystem
    return dest
