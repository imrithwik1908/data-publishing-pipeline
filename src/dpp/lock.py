"""A file lock so two pipeline runs can never publish at the same time."""

from __future__ import annotations

import fcntl
from contextlib import contextmanager
from pathlib import Path


class PipelineLocked(RuntimeError):
    pass


@contextmanager
def pipeline_lock(lock_path: Path):
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with open(lock_path, "w") as fh:
        try:
            # Non-blocking: fail fast instead of queueing silently behind another run.
            fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise PipelineLocked(f"another pipeline run holds {lock_path}") from exc
        try:
            yield
        finally:
            fcntl.flock(fh, fcntl.LOCK_UN)
