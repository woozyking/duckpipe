"""Optional fsspec-backed remote state sync (DESIGN.md sec 2 tenet #1, sec 9).

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
server, no catalog database. See DESIGN.md sec 11 for
why this beat both a persistent Quack server and a full DuckLake catalog
for this specific problem.

``write_delta()``/``absorb_pending()`` extend the same idea to Phase 3a's
task-scoped (``only=``) runs (DESIGN.md sec 8): instead of contending
for the whole state file, a scoped run drops its own new rows in a
uniquely-named file under ``<state_uri>.pending/`` -- a unique key can
never collide with anyone else's, so this needs no lock at all -- and any
whole-file sync absorbs whatever's pending first.
"""

from __future__ import annotations

import contextlib
import json
import logging
import os
import socket
import tempfile
import time
import uuid
from collections.abc import Callable
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

    # Local filesystems need the parent directory to exist before an
    # exclusive create; object stores have no such concept, so this is a
    # no-op there.
    fs.makedirs(fs._parent(lock_path), exist_ok=True)

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
                f"same state_uri (DESIGN.md sec 11)"
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
    fs.makedirs(fs._parent(remote_path), exist_ok=True)
    fs.put_file(str(local_path), remote_path)


def _pending_prefix(state_uri: str) -> str:
    return state_uri + ".pending/"


def write_delta(state_uri: str, local_delta_path: str | Path) -> None:
    """Upload one scoped (``only=``) run's own new rows as a uniquely-keyed
    object under ``<state_uri>.pending/`` -- never a name anyone else could
    also pick, so this never needs a lock."""
    import fsspec

    fs, prefix = fsspec.core.url_to_fs(_pending_prefix(state_uri))
    fs.makedirs(prefix, exist_ok=True)
    fs.put_file(str(local_delta_path), f"{prefix.rstrip('/')}/{uuid.uuid4().hex}.duckdb")


def absorb_pending(state_uri: str, merge: Callable[[Path], None], *, delete: bool = False) -> None:
    """Merge every pending delta under ``<state_uri>.pending/`` by calling
    ``merge(local_path)`` once per file. ``merge`` is typically
    ``StateStore.absorb_delta``; kept as a callback here so this module
    stays fsspec-only and never needs to import ``state.py``.

    ``delete=False`` (the default, used by scoped ``only=`` runs) leaves
    every delta in place: a scoped run only ever merges into its own local
    scratch copy, and deleting what it just read would rob any other
    worker that hasn't started yet of the only durable copy of that fact.
    ``delete=True`` is only for a whole-run absorb (``run()`` without
    ``only=``, or ``compact()``): it's about to re-upload a fully merged
    file under the whole-run lock, which is what actually makes deleting
    the now-redundant deltas safe.
    """
    import fsspec

    fs, prefix = fsspec.core.url_to_fs(_pending_prefix(state_uri))
    try:
        pending = [p for p in fs.ls(prefix, detail=False) if p.endswith(".duckdb")]
    except FileNotFoundError:
        return

    for obj in pending:
        with tempfile.TemporaryDirectory() as tmp_dir:
            local = Path(tmp_dir) / "delta.duckdb"
            fs.get_file(obj, str(local))
            merge(local)
        if delete:
            fs.rm(obj)


def compact(state_uri: str, *, db_path: str | Path | None = None, lock: bool = True) -> None:
    """Fold every pending delta into the canonical state file and clean up
    ``.pending/``. Nothing requires this -- every invocation already
    absorbs pending deltas itself -- but a purely distributed workflow
    (many ``--only`` workers, no whole-run ever) never otherwise re-uploads
    the canonical file, so ``.pending/`` only grows. Safe to run anytime,
    e.g. periodically from cron alongside the pipeline itself.
    """
    import tempfile as _tempfile

    from duckpipe.state import StateStore

    with _tempfile.TemporaryDirectory() as tmp_dir:
        local_path = Path(db_path) if db_path is not None else Path(tmp_dir) / "duckpipe.db"
        with contextlib.ExitStack() as stack:
            if lock:
                stack.enter_context(locked(state_uri))
            sync_down(state_uri, local_path)
            with StateStore(local_path) as store:
                absorb_pending(state_uri, store.absorb_delta, delete=True)
            sync_up(state_uri, local_path)
