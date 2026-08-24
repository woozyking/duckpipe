"""Optional fsspec-backed remote state sync (ROADMAP.md sec 2 tenet #1, sec 9).

DuckDB's own database file format only supports read-only remote
``ATTACH``; read-write access is filesystem-local only. So instead of
attaching the state file live, DuckPipe downloads it to local scratch
before a run and uploads it back after. Imported lazily, only when a
``state_uri`` is actually configured, so the core dependency tree never
requires ``fsspec`` unless a user opts in (``duckpipe[remote]``/``[s3]``/
``[gcs]``/``[azure]``).

``locked()`` closes the concurrent-writer race a naive download-mutate-
upload cycle would otherwise have: an advisory lock *object*, written via
each backend's own native conditional-write primitive (S3
``If-None-Match``, GCS ``if_generation_match``, Azure ETag preconditions)
through fsspec's standard exclusive-create (``"x"``) file mode -- no
server, no catalog database. See ROADMAP.md sec 12, open question #5 for
why this beat both a persistent Quack server and a full DuckLake catalog
for this specific problem.
"""

from __future__ import annotations

import contextlib
import json
import logging
import os
import socket
import time
from pathlib import Path
from typing import Any

logger = logging.getLogger("duckpipe")

DEFAULT_MAX_LOCK_AGE = 3600.0  # seconds; a lock older than this is treated as abandoned


class StateLockedError(RuntimeError):
    """Another invocation already holds the lock for this ``state_uri``."""


def _lock_uri(state_uri: str) -> str:
    return state_uri + ".lock"


def _holder_id() -> str:
    return f"{socket.gethostname()}:{os.getpid()}"


def _read_lock(fs: Any, lock_path: str) -> dict[str, Any]:
    with fs.open(lock_path, "rb") as f:
        return json.loads(f.read().decode())


@contextlib.contextmanager
def locked(state_uri: str, *, max_lock_age: float = DEFAULT_MAX_LOCK_AGE):
    """Hold an advisory lock on ``state_uri`` for the duration of the
    ``with`` block. Raises ``StateLockedError`` if another, not-yet-stale
    invocation already holds it. A lock older than ``max_lock_age``
    seconds is assumed abandoned (e.g. a crashed process that never
    reached the ``finally``) and is reclaimed instead, with a warning.
    """
    import fsspec

    fs, lock_path = fsspec.core.url_to_fs(_lock_uri(state_uri))
    payload = json.dumps({"holder": _holder_id(), "acquired_at": time.time()}).encode()

    try:
        with fs.open(lock_path, "xb") as f:
            f.write(payload)
    except FileExistsError:
        existing = _read_lock(fs, lock_path)
        age = time.time() - existing.get("acquired_at", 0)
        if age <= max_lock_age:
            raise StateLockedError(
                f"{state_uri} is locked by {existing.get('holder', '?')} "
                f"({age:.0f}s ago); refusing to run concurrently against the "
                f"same state_uri (ROADMAP.md sec 12, open question #5)"
            ) from None
        # Stale lock (holder likely crashed without releasing) -- reclaim it.
        # Not perfectly race-free against another reclaimer at this exact
        # instant, but that's the same order of risk any lease-based lock
        # accepts, and far better than no lock at all.
        logger.warning(
            "reclaiming stale lock on %s held by %s (%.0fs old)",
            state_uri,
            existing.get("holder", "?"),
            age,
        )
        fs.rm(lock_path)
        with fs.open(lock_path, "xb") as f:
            f.write(payload)

    try:
        yield
    finally:
        fs.rm(lock_path)


def sync_down(state_uri: str, local_path: str | Path) -> None:
    import fsspec

    local_path = Path(local_path)
    fs, remote_path = fsspec.core.url_to_fs(state_uri)
    if fs.exists(remote_path):
        local_path.parent.mkdir(parents=True, exist_ok=True)
        fs.get_file(remote_path, str(local_path))


def sync_up(state_uri: str, local_path: str | Path) -> None:
    import fsspec

    fs, remote_path = fsspec.core.url_to_fs(state_uri)
    fs.put_file(str(local_path), remote_path)
