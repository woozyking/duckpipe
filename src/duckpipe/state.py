"""DuckDB-backed state store -- the orchestrator's entire "infrastructure".

Every run, every task's status/duration/fingerprint/error is a row,
queryable with ``SELECT * FROM task_runs`` from any DuckDB client -- no
separate UI or metadata service required (ROADMAP.md sec 5).

Two backends share this exact same schema and Python API:

- The default: a plain ``.duckdb`` file (``db_path="pipeline.duckdb"``).
- Opt-in (ROADMAP.md sec 8, Phase 3b): a DuckLake catalog
  (``db_path="ducklake:sqlite:pipeline.ducklake.sqlite"``), for teams that
  want real snapshot history -- time travel over every run, schema
  evolution with no migration step, other tools querying the same
  catalog. Nothing about calling ``duckpipe.run(...)`` changes; it's the
  same ``db_path``/``--db`` you'd hand a plain file, pointed at a
  different kind of string. The same catalog string can instead point at
  a dedicated, long-running metadata database you already run --
  ``db_path="ducklake:postgres:dbname=... host=..."`` -- with ``data_path``
  passed explicitly, giving several DuckPipe deployments one shared,
  concurrently-writable catalog (verified directly: 8/8 concurrent
  commits succeed against a real Postgres catalog with zero retry logic,
  vs. 3/8 failing outright against a SQLite one under the same load --
  see ``examples/05_distributed_with_ducklake``). Nothing here requires
  standing that up; it's there for teams that already have one and want
  a shared, multi-user/multi-tenant catalog behind several DuckPipe
  deployments. ``only=``/``state_uri`` (Phase 3a's distributed mechanism)
  are deliberately not wired to this backend either way -- see
  ``scheduler.run()``'s docstring for why.

Neither ``PRIMARY KEY`` nor ``ON CONFLICT`` appears anywhere in this
schema: DuckLake doesn't support constraints at all (verified directly
against DuckLake 1.x, not assumed), so every "upsert" here is a portable
delete-then-insert or insert-where-not-exists instead -- one dialect, no
per-backend branching, and no practical cost at the row counts this
schema ever holds.
"""

from __future__ import annotations

import contextlib
import pickle
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id VARCHAR,
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
    task_name VARCHAR,
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
    created_at TIMESTAMP
);
"""

# `CREATE VIEW IF NOT EXISTS`, not `CREATE OR REPLACE` -- verified directly
# that against a DuckLake-backed store, `CREATE OR REPLACE VIEW` commits a
# new snapshot *every time a StateStore opens*, even when the view is
# byte-identical to before. That would flood exactly the run-history
# story this schema exists to make useful. The tradeoff: a future
# duckpipe version that changes a view's SQL won't retroactively update
# an existing state file's view -- `DROP VIEW` and reopen if you need
# that.
VIEWS_SQL = """
CREATE VIEW IF NOT EXISTS v_latest_task_status AS
SELECT run_id, task_name, status, started_at, ended_at, duration_ms, error, fingerprint
FROM task_runs
QUALIFY row_number() OVER (PARTITION BY task_name ORDER BY ended_at DESC) = 1;

CREATE VIEW IF NOT EXISTS v_run_summary AS
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

CREATE VIEW IF NOT EXISTS v_task_stats AS
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

DUCKLAKE_PREFIX = "ducklake:"
_FILE_CATALOG_PREFIXES = ("ducklake:sqlite:", "ducklake:duckdb:")


def is_ducklake(db_path: str | Path) -> bool:
    """Whether ``db_path`` names a DuckLake catalog (Phase 3b) rather than
    a plain local file."""
    return isinstance(db_path, str) and db_path.startswith(DUCKLAKE_PREFIX)


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


def _default_data_path(catalog: str) -> str | None:
    """A DuckLake ``DATA_PATH`` is mandatory (verified: DuckLake refuses
    to create a catalog without one) but derivable for a local
    file-backed catalog -- a sibling ``<file>.data/`` directory, same
    spirit as ``duckpipe.db`` appearing next to a pipeline (tenet #4).
    Returns ``None`` for anything else (Postgres/MySQL catalogs have no
    file to derive a sibling from), so the caller can raise clearly.

    Always resolved to an absolute path: DuckLake stores the ``DATA_PATH``
    it was created with and rejects a later ``ATTACH`` whose ``DATA_PATH``
    doesn't match *textually* (verified directly) -- two invocations
    that happen to spell the same catalog file differently (a relative
    path from a different ``cwd``, say) would otherwise derive two
    different-looking paths for what both mean.
    """
    for prefix in _FILE_CATALOG_PREFIXES:
        if catalog.startswith(prefix):
            catalog_file = Path(catalog[len(prefix) :]).resolve()
            return str(catalog_file.parent / f"{catalog_file.name}.data")
    return None


def _install_catalog_extension(con: duckdb.DuckDBPyConnection, catalog: str) -> None:
    lowered = catalog.lower()
    if "sqlite:" in lowered:
        con.execute("INSTALL sqlite")
    elif "postgres" in lowered:
        con.execute("INSTALL postgres")
    elif "mysql:" in lowered:
        con.execute("INSTALL mysql")
    # A "ducklake:duckdb:..." catalog needs no extra extension -- DuckDB
    # is the engine itself.


class StateStore:
    """Thin wrapper around one DuckDB connection holding all orchestrator state."""

    def __init__(
        self,
        db_path: str | Path,
        *,
        data_path: str | None = None,
        read_only: bool = False,
    ) -> None:
        self.db_path = db_path
        self.is_ducklake = is_ducklake(db_path)
        self.read_only = read_only
        self.con = (
            self._attach_ducklake(str(db_path), data_path, read_only=read_only)
            if self.is_ducklake
            else self._open_local(Path(db_path), read_only=read_only)
        )
        if not read_only:
            # One tagged snapshot for first-ever setup (a no-op, so no new
            # snapshot at all, on every open after the first -- both
            # tables and views use IF NOT EXISTS) instead of one
            # anonymous snapshot per CREATE statement.
            with self.transaction("duckpipe: initialize schema"):
                self.con.execute(SCHEMA_SQL)
                self.con.execute(VIEWS_SQL)

    @staticmethod
    def _open_local(path: Path, *, read_only: bool) -> duckdb.DuckDBPyConnection:
        if not read_only:
            path.parent.mkdir(parents=True, exist_ok=True)
        return duckdb.connect(str(path), read_only=read_only)

    @staticmethod
    def _attach_ducklake(
        catalog: str, data_path: str | None, *, read_only: bool
    ) -> duckdb.DuckDBPyConnection:
        con = duckdb.connect()
        con.execute("INSTALL ducklake")  # needs network on a cold extension cache
        _install_catalog_extension(con, catalog)
        con.execute("LOAD ducklake")

        if read_only:
            # An existing catalog remembers its own DATA_PATH -- verified
            # directly, re-specifying it for a read-only reattach isn't
            # needed (or wanted: a read-only caller may not even know it).
            con.execute(f"ATTACH '{catalog}' AS dl (READ_ONLY)")
            con.execute("USE dl")
            return con

        for prefix in _FILE_CATALOG_PREFIXES:
            if catalog.startswith(prefix):
                Path(catalog[len(prefix) :]).parent.mkdir(parents=True, exist_ok=True)
                break

        data_path = data_path or _default_data_path(catalog)
        if data_path is None:
            raise ValueError(
                f"data_path is required for this DuckLake catalog ({catalog!r}) -- "
                "duckpipe can only derive a default data directory for a local "
                "ducklake:sqlite:/ducklake:duckdb: catalog file. A "
                "Postgres/MySQL-backed catalog works the same way otherwise -- "
                "just pass data_path explicitly (a shared network path or "
                "object-storage URI every deployment can reach)"
            )
        if "://" not in data_path:
            Path(data_path).mkdir(parents=True, exist_ok=True)

        con.execute(f"ATTACH '{catalog}' AS dl (DATA_PATH '{data_path}')")
        con.execute("USE dl")
        return con

    def close(self) -> None:
        self.con.close()

    def __enter__(self) -> StateStore:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    @contextlib.contextmanager
    def transaction(self, message: str | None = None):
        """Group several writes into one atomic commit. Against a
        DuckLake-backed store, ``message`` also tags the resulting
        snapshot (the ``commit_message`` column ``snapshots()`` returns)
        -- the difference between time travel being a bare list of
        anonymous versions and an actually-readable run history.
        """
        self.con.execute("BEGIN TRANSACTION")
        try:
            if self.is_ducklake and message:
                self.con.execute("CALL dl.set_commit_message(?, ?)", ["duckpipe", message])
            yield
        except Exception:
            self.con.execute("ROLLBACK")
            raise
        else:
            self.con.execute("COMMIT")

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
        with self.transaction(f"pipeline run started ({module_path})"):
            self.con.execute(
                "INSERT INTO pipeline_runs SELECT ?, ?, ?, NULL, 'running' "
                "WHERE NOT EXISTS (SELECT 1 FROM pipeline_runs WHERE run_id = ?)",
                [run_id, module_path, now(), run_id],
            )
        return run_id

    def finish_run(self, run_id: str, status: str) -> None:
        with self.transaction(f"pipeline run finished: {status}"):
            self.con.execute(
                "UPDATE pipeline_runs SET ended_at = ?, status = ? WHERE run_id = ?",
                [now(), status, run_id],
            )

    # -- lineage ------------------------------------------------------------

    def record_lineage(self, task_name: str, upstream_names: list[str]) -> None:
        """Record ``task_name``'s upstream set. Deliberately not wrapped in
        its own ``transaction()`` -- callers record a task's lineage and
        its outcome (``record_task_run``/``set_fingerprint``/etc.) inside
        one shared transaction, so a DuckLake-backed store commits both
        together as a single, meaningfully-tagged snapshot rather than two
        anonymous ones.
        """
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
        self.con.execute("DELETE FROM task_fingerprints WHERE task_name = ?", [task_name])
        self.con.execute(
            "INSERT INTO task_fingerprints VALUES (?, ?, ?)",
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
            "DELETE FROM task_cache WHERE task_name = ? AND fingerprint = ?",
            [task_name, fingerprint],
        )
        self.con.execute(
            "INSERT INTO task_cache VALUES (?, ?, ?, ?, ?, ?)",
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
                "INSERT INTO pipeline_runs SELECT * FROM _delta.pipeline_runs t "
                "WHERE NOT EXISTS (SELECT 1 FROM pipeline_runs m WHERE m.run_id = t.run_id)"
            )
            self.con.execute(
                "INSERT INTO task_runs SELECT * FROM _delta.task_runs t "
                "WHERE NOT EXISTS ("
                "  SELECT 1 FROM task_runs m WHERE m.run_id = t.run_id "
                "  AND m.task_name = t.task_name AND m.attempt = t.attempt"
                ")"
            )
            # Fingerprints/lineage should reflect the delta's version when
            # present (an upsert), not merely fill gaps -- delete-then-
            # insert per task_name, the same idiom set_fingerprint()/
            # record_lineage() already use locally.
            names = self.con.execute(
                "SELECT DISTINCT task_name FROM _delta.task_fingerprints"
            ).fetchall()
            for (name,) in names:
                self.con.execute("DELETE FROM task_fingerprints WHERE task_name = ?", [name])
            self.con.execute("INSERT INTO task_fingerprints SELECT * FROM _delta.task_fingerprints")

            self.con.execute(
                "INSERT INTO task_cache SELECT * FROM _delta.task_cache t "
                "WHERE NOT EXISTS ("
                "  SELECT 1 FROM task_cache m WHERE m.task_name = t.task_name "
                "  AND m.fingerprint = t.fingerprint"
                ")"
            )
            names = self.con.execute(
                "SELECT DISTINCT task_name FROM _delta.task_lineage"
            ).fetchall()
            for (name,) in names:
                self.con.execute("DELETE FROM task_lineage WHERE task_name = ?", [name])
            self.con.execute("INSERT INTO task_lineage SELECT * FROM _delta.task_lineage")
        finally:
            self.con.execute("DETACH _delta")

    # -- observability upgrade (Phase 3b) --------------------------------

    def snapshots(self) -> list[dict[str, Any]]:
        """Every snapshot this store has ever committed -- the
        time-travel-over-run-history payoff of the DuckLake backend.
        Query any table ``AT (VERSION => snapshot_id)`` to see state as
        of that point. Raises on a plain-file store, which has no
        snapshot concept; check ``.is_ducklake`` first if you need to
        branch on it.
        """
        if not self.is_ducklake:
            raise RuntimeError(
                "snapshots() needs a DuckLake-backed store (db_path='ducklake:...'); "
                "this store is a plain .duckdb file, which has no snapshot history"
            )
        rows = self.con.execute(
            "SELECT snapshot_id, snapshot_time, author, commit_message "
            "FROM ducklake_snapshots('dl') ORDER BY snapshot_id"
        ).fetchall()
        return [
            {"snapshot_id": r[0], "snapshot_time": r[1], "author": r[2], "commit_message": r[3]}
            for r in rows
        ]
