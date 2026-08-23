"""DuckDB-backed state store -- the orchestrator's entire "infrastructure".

Every run, every task's status/duration/fingerprint/error is a row in a
plain ``.duckdb`` file next to the pipeline (ROADMAP.md sec 5), queryable
with ``SELECT * FROM task_runs`` from any DuckDB client -- no separate UI
or metadata service required.
"""

from __future__ import annotations

import pickle
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id VARCHAR PRIMARY KEY,
    module_path VARCHAR,
    started_at TIMESTAMP,
    ended_at TIMESTAMP,
    status VARCHAR
);

CREATE TABLE IF NOT EXISTS task_runs (
    run_id VARCHAR,
    task_name VARCHAR,
    status VARCHAR,
    started_at TIMESTAMP,
    ended_at TIMESTAMP,
    duration_ms DOUBLE,
    error VARCHAR,
    fingerprint VARCHAR,
    skipped BOOLEAN,
    attempt INTEGER
);

CREATE TABLE IF NOT EXISTS task_fingerprints (
    task_name VARCHAR PRIMARY KEY,
    fingerprint VARCHAR,
    updated_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS task_lineage (
    task_name VARCHAR,
    upstream_task_name VARCHAR
);

CREATE TABLE IF NOT EXISTS task_cache (
    task_name VARCHAR,
    fingerprint VARCHAR,
    backend VARCHAR,
    payload BLOB,
    size_bytes BIGINT,
    created_at TIMESTAMP,
    PRIMARY KEY (task_name, fingerprint)
);
"""

# See ROADMAP.md open question #3: pickling is simple and fully generic but
# can be slow/large for big tabular outputs -- warn rather than silently
# writing a huge blob into the state file.
LARGE_CACHE_WARN_BYTES = 50 * 1024 * 1024


def now() -> datetime:
    return datetime.now(UTC)


def new_run_id() -> str:
    return uuid.uuid4().hex


class StateStore:
    """Thin wrapper around one DuckDB connection holding all orchestrator state."""

    def __init__(self, db_path: str | Path) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.con = duckdb.connect(str(self.db_path))
        self.con.execute(SCHEMA_SQL)

    def close(self) -> None:
        self.con.close()

    def __enter__(self) -> StateStore:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # -- pipeline-level ---------------------------------------------------

    def start_run(self, module_path: str) -> str:
        run_id = new_run_id()
        self.con.execute(
            "INSERT INTO pipeline_runs VALUES (?, ?, ?, NULL, 'running')",
            [run_id, module_path, now()],
        )
        return run_id

    def finish_run(self, run_id: str, status: str) -> None:
        self.con.execute(
            "UPDATE pipeline_runs SET ended_at = ?, status = ? WHERE run_id = ?",
            [now(), status, run_id],
        )

    # -- lineage ------------------------------------------------------------

    def record_lineage(self, task_name: str, upstream_names: list[str]) -> None:
        self.con.execute("DELETE FROM task_lineage WHERE task_name = ?", [task_name])
        for up in upstream_names:
            self.con.execute("INSERT INTO task_lineage VALUES (?, ?)", [task_name, up])

    # -- fingerprints ---------------------------------------------------------

    def get_fingerprint(self, task_name: str) -> str | None:
        row = self.con.execute(
            "SELECT fingerprint FROM task_fingerprints WHERE task_name = ?",
            [task_name],
        ).fetchone()
        return row[0] if row else None

    def set_fingerprint(self, task_name: str, fingerprint: str) -> None:
        self.con.execute(
            """
            INSERT INTO task_fingerprints VALUES (?, ?, ?)
            ON CONFLICT (task_name) DO UPDATE SET
                fingerprint = excluded.fingerprint,
                updated_at = excluded.updated_at
            """,
            [task_name, fingerprint, now()],
        )

    # -- task runs ------------------------------------------------------

    def record_task_run(
        self,
        run_id: str,
        task_name: str,
        status: str,
        started_at: datetime,
        ended_at: datetime,
        fingerprint: str,
        error: str | None = None,
        skipped: bool = False,
        attempt: int = 1,
    ) -> None:
        duration_ms = (ended_at - started_at).total_seconds() * 1000
        self.con.execute(
            "INSERT INTO task_runs VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                run_id,
                task_name,
                status,
                started_at,
                ended_at,
                duration_ms,
                error,
                fingerprint,
                skipped,
                attempt,
            ],
        )

    def last_status(self, task_name: str) -> tuple[str, datetime] | None:
        row = self.con.execute(
            """
            SELECT status, ended_at FROM task_runs
            WHERE task_name = ? ORDER BY ended_at DESC LIMIT 1
            """,
            [task_name],
        ).fetchone()
        return (row[0], row[1]) if row else None

    # -- cache ------------------------------------------------------------

    def has_cached(self, task_name: str, fingerprint: str) -> bool:
        row = self.con.execute(
            "SELECT 1 FROM task_cache WHERE task_name = ? AND fingerprint = ?",
            [task_name, fingerprint],
        ).fetchone()
        return row is not None

    def get_cached(self, task_name: str, fingerprint: str) -> Any:
        row = self.con.execute(
            "SELECT payload FROM task_cache WHERE task_name = ? AND fingerprint = ?",
            [task_name, fingerprint],
        ).fetchone()
        if row is None:
            raise KeyError((task_name, fingerprint))
        return pickle.loads(row[0])

    def set_cached(
        self, task_name: str, fingerprint: str, value: Any, backend: str = "pickle"
    ) -> int:
        payload = pickle.dumps(value)
        size = len(payload)
        self.con.execute(
            """
            INSERT INTO task_cache VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT (task_name, fingerprint) DO UPDATE SET
                payload = excluded.payload,
                size_bytes = excluded.size_bytes,
                created_at = excluded.created_at
            """,
            [task_name, fingerprint, backend, payload, size, now()],
        )
        return size
