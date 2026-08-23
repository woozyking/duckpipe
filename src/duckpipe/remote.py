"""Optional fsspec-backed remote state sync (ROADMAP.md sec 2 tenet #1, sec 9).

DuckDB's own database file format only supports read-only remote
``ATTACH``; read-write access is filesystem-local only. So instead of
attaching the state file live, DuckPipe downloads it to local scratch
before a run and uploads it back after -- this is the correct pattern for
a read-write ``.duckdb`` file backed by object storage, not a workaround.
It's what makes DuckPipe safe to nest inside another orchestrator's
ephemeral, container-per-task workers.

Imported lazily, only when a ``state_uri`` is actually configured, so the
core dependency tree never requires ``fsspec`` unless a user opts in
(install with the ``duckpipe[remote]``/``[s3]``/``[gcs]``/``[azure]`` extra).

Concurrent-writer caveat (ROADMAP.md open question #5): two overlapping
invocations against the same ``state_uri`` race on download-mutate-upload;
v1 accepts last-writer-wins. Don't point two concurrent runs at the same
``state_uri`` unless that's acceptable.
"""

from __future__ import annotations

from pathlib import Path


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
