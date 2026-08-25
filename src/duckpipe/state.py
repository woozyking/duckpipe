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
    status VARCHAR,  -- 'success' | 'skipped' | 'failed' | 'upstream_failed'
    started_at TIMESTAMP,
    ended_at TIMESTAMP,
    duration_ms DOUBLE,
    error VARCHAR,
    fingerprint VARCHAR,
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

# Pre-built observability views (ROADMAP.md Phase 2: "since state is just
# DuckDB, ship a couple of pre-built SQL views... rather than building a UI
# server"). `duckpipe stats` queries these directly, but they're just as
# usable from any other DuckDB client against the same .duckdb file.
VIEWS_SQL = """
CREATE OR REPLACE VIEW v_latest_task_status AS
SELECT run_id, task_name, status, started_at, ended_at, duration_ms, error, fingerprint
FROM task_runs
QUALIFY row_number() OVER (PARTITION BY task_name ORDER BY ended_at DESC) = 1;

CREATE OR REPLACE VIEW v_run_summary AS
SELECT
    p.run_id,
    p.module_path,
    p.started_at,
    p.ended_at,
    p.status,
    count(t.task_name) AS task_count,
    sum(CASE WHEN t.status = 'success' THEN 1 ELSE 0 END) AS succeeded_count,
    sum(CASE WHEN t.status = 'skipped' THEN 1 ELSE 0 END) AS skipped_count,
    sum(CASE WHEN t.status IN ('failed', 'upstream_failed') THEN 1 ELSE 0 END) AS failed_count
FROM pipeline_runs p
LEFT JOIN task_runs t ON t.run_id = p.run_id
GROUP BY p.run_id, p.module_path, p.started_at, p.ended_at, p.status;

CREATE OR REPLACE VIEW v_task_stats AS
SELECT
    task_name,
    count(*) AS runs,
    avg(duration_ms) AS avg_duration_ms,
    max(duration_ms) AS max_duration_ms,
    sum(CASE WHEN status = 'skipped' THEN 1 ELSE 0 END) AS skipped_count,
    sum(CASE WHEN status IN ('failed', 'upstream_failed') THEN 1 ELSE 0 END) AS failed_count,
    max(ended_at) AS last_run_at
FROM task_runs
GROUP BY task_name;
"""

# See ROADMAP.md open question #3: pickling is simple and fully generic but
# can be slow/large for big tabular outputs -- warn rather than silently
# writing a huge blob into the state file.
LARGE_CACHE_WARN_BYTES = 50 * 1024 * 1024


def now() -> datetime:
    return datetime.now(UTC)


def new_run_id() -> str:
    return uuid.uuid4().hex


def serialize(value: Any, backend: str) -> bytes:
    if backend == "pickle":
        return pickle.dumps(value)
    if backend == "arrow":
        return _serialize_arrow(value)
    raise ValueError(f"unknown cache_backend {backend!r}")


def deserialize(payload: bytes, backend: str) -> Any:
    if backend == "pickle":
        return pickle.loads(payload)
    if backend == "arrow":
        return _deserialize_arrow(payload)
    raise ValueError(f"unknown cache_backend {backend!r}")


def _serialize_arrow(value: Any) -> bytes:
    # Lazy import: the core dependency tree never requires pyarrow unless
    # a task actually opts into `cache_backend="arrow"` (duckpipe[arrow]).
    # `pa.table(value)` accepts anything implementing the Arrow PyCapsule
    # interface -- a DuckDBPyRelation, a pandas/Polars DataFrame, a
    # pyarrow Table itself (ROADMAP.md sec 6.2, sec 13). Daft doesn't
    # implement it yet as of this writing (Eventual-Inc/Daft#2504); call
    # `.to_arrow()` yourself first if you need to cache a Daft result.
    import pyarrow as pa

    table = pa.table(value)
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return sink.getvalue().to_pybytes()


def _deserialize_arrow(payload: bytes) -> Any:
    import pyarrow as pa

    # A cache hit always hands back a plain pyarrow.Table regardless of
    # what type originally produced it -- caching is inherently an eager
    # materialization step, and pyarrow.Table interoperates trivially
    # with DuckDB (`duckdb.sql("select * from table")`), Polars
    # (`pl.from_arrow`), and pandas (`.to_pandas()`) from there.
    with pa.ipc.open_stream(payload) as reader:
        return reader.read_all()


class StateStore:
    """Thin wrapper around one DuckDB connection holding all orchestrator state."""

    def __init__(self, db_path: str | Path) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.con = duckdb.connect(str(self.db_path))
        self.con.execute(SCHEMA_SQL)
        self.con.execute(VIEWS_SQL)

    def close(self) -> None:
        self.con.close()

    def __enter__(self) -> StateStore:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # -- pipeline-level ---------------------------------------------------

    def start_run(self, module_path: str, run_id: str | None = None) -> str:
        """Start (or, with an explicit ``run_id``, join) a pipeline run.

        A coordinator dispatching one distributed run as several scoped
        (``only=``) invocations passes the same ``run_id`` to each so they
        group under one row in ``pipeline_runs`` -- the insert is a no-op
        if that row already exists, whether because another scoped
        invocation already created it locally, or because it arrived via
        ``absorb_delta`` from one that ran elsewhere.
        """
        run_id = run_id or new_run_id()
        self.con.execute(
            "INSERT INTO pipeline_runs VALUES (?, ?, ?, NULL, 'running') "
            "ON CONFLICT (run_id) DO NOTHING",
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
        attempt: int = 1,
    ) -> None:
        duration_ms = (ended_at - started_at).total_seconds() * 1000
        self.con.execute(
            "INSERT INTO task_runs VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                run_id,
                task_name,
                status,
                started_at,
                ended_at,
                duration_ms,
                error,
                fingerprint,
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
            "SELECT payload, backend FROM task_cache WHERE task_name = ? AND fingerprint = ?",
            [task_name, fingerprint],
        ).fetchone()
        if row is None:
            raise KeyError((task_name, fingerprint))
        payload, backend = row
        return deserialize(payload, backend)

    def set_cached(
        self, task_name: str, fingerprint: str, value: Any, backend: str = "pickle"
    ) -> int:
        payload = serialize(value, backend)
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

    # -- distributed (Phase 3a) ------------------------------------------

    def absorb_delta(self, delta_path: str | Path) -> None:
        """Merge a delta file -- the rows one scoped (``only=``) invocation
        produced -- into this store. A delta is just another
        ``duckpipe.db`` (same schema, via ``StateStore.__init__``), so
        absorbing one is ``ATTACH`` plus a few ``INSERT``s, not a new
        format. Every table merges idempotently, since the same delta
        could plausibly be absorbed more than once (e.g. two whole-run
        invocations racing to absorb the same pending file).
        """
        self.con.execute(f"ATTACH '{delta_path}' AS _delta (READ_ONLY)")
        try:
            self.con.execute(
                "INSERT INTO pipeline_runs SELECT * FROM _delta.pipeline_runs "
                "ON CONFLICT (run_id) DO NOTHING"
            )
            self.con.execute(
                "INSERT INTO task_runs SELECT * FROM _delta.task_runs t "
                "WHERE NOT EXISTS ("
                "  SELECT 1 FROM task_runs m WHERE m.run_id = t.run_id "
                "  AND m.task_name = t.task_name AND m.attempt = t.attempt"
                ")"
            )
            self.con.execute(
                "INSERT INTO task_fingerprints SELECT * FROM _delta.task_fingerprints "
                "ON CONFLICT (task_name) DO UPDATE SET "
                "fingerprint = excluded.fingerprint, updated_at = excluded.updated_at"
            )
            self.con.execute(
                "INSERT INTO task_cache SELECT * FROM _delta.task_cache "
                "ON CONFLICT (task_name, fingerprint) DO NOTHING"
            )
            # task_lineage has no primary key (a task's upstream set is
            # replaced wholesale on every record_lineage() call, matching
            # what record_lineage itself does locally) -- delete-then-insert
            # per task_name so re-absorbing the same delta never duplicates.
            names = self.con.execute(
                "SELECT DISTINCT task_name FROM _delta.task_lineage"
            ).fetchall()
            for (name,) in names:
                self.con.execute("DELETE FROM task_lineage WHERE task_name = ?", [name])
            self.con.execute("INSERT INTO task_lineage SELECT * FROM _delta.task_lineage")
        finally:
            self.con.execute("DETACH _delta")
