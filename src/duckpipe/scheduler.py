"""DAG-level concurrency and the top-level ``run()`` entrypoint.

Independent tasks run concurrently via a plain ``asyncio`` + thread-pool
executor (ROADMAP.md tenet #3) -- never a second parallelism model layered
on top of what a task's own engine (DuckDB/Polars/etc.) already does
internally. Everything about a task's *internal* parallelism stays the
user's own code.

``run()`` is the one function every trigger -- cron, CI, a webhook
handler, a Lambda entrypoint, or a single step embedded inside Airflow /
Prefect / Dagster (sec 9) -- needs to call.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from dataclasses import dataclass, field
from pathlib import Path
from types import ModuleType
from typing import Any, Literal

from duckpipe.dag import DAG, build_dag
from duckpipe.fingerprint import resolve_fingerprints
from duckpipe.state import LARGE_CACHE_WARN_BYTES, StateStore, is_ducklake, new_run_id, now
from duckpipe.task import Task

logger = logging.getLogger("duckpipe")

Status = Literal["success", "skipped", "failed", "upstream_failed"]
_FAILURE_STATUSES = ("failed", "upstream_failed")


class DuckLakeCombinationError(ValueError):
    """A DuckLake ``db_path`` was combined with something it deliberately
    doesn't support (ROADMAP.md sec 8): it's a local observability
    upgrade, not a rework of Phase 3a's distributed mechanism."""


class UpstreamNotReadyError(RuntimeError):
    """A scoped (``only=``) run's upstream task hasn't completed with its
    current code yet -- the coordinator dispatched it out of order."""


class UpstreamNotCachedError(RuntimeError):
    """A scoped (``only=``) run needs an upstream task's *value*, but that
    upstream has no ``cache=True`` to have persisted one."""


@dataclass
class RunSummary:
    run_id: str
    db_path: str | Path
    results: dict[str, Any] = field(default_factory=dict)
    statuses: dict[str, Status] = field(default_factory=dict)
    errors: dict[str, str] = field(default_factory=dict)

    @property
    def success(self) -> bool:
        return all(s not in _FAILURE_STATUSES for s in self.statuses.values())

    def __repr__(self) -> str:
        return f"RunSummary(run_id={self.run_id!r}, statuses={self.statuses!r})"


def would_skip(store: StateStore, t: Task, fingerprint: str) -> bool:
    """Whether ``t`` would be skipped (cache hit) if run right now with this
    fingerprint. Shared by the scheduler's actual skip check and
    ``duckpipe show``'s "what would happen next" preview, so the two can
    never silently disagree.
    """
    if not t.cache or store.get_fingerprint(t.name) != fingerprint:
        return False
    return store.has_cached(t.name, fingerprint)


async def _run_with_retries(t: Task, kwargs: dict[str, Any]) -> Any:
    loop = asyncio.get_running_loop()
    last_exc: BaseException | None = None
    for attempt in range(t.retries + 1):
        try:
            return await loop.run_in_executor(None, lambda: t.func(**kwargs))
        except Exception as exc:
            last_exc = exc
            logger.warning(
                "task %s attempt %d/%d failed: %s", t.name, attempt + 1, t.retries + 1, exc
            )
            if attempt < t.retries and t.retry_delay:
                await asyncio.sleep(t.retry_delay)
    assert last_exc is not None
    raise last_exc


async def _execute_dag(
    dag: DAG,
    store: StateStore,
    run_id: str,
    force: bool,
    max_workers: int | None,
) -> RunSummary:
    order = dag.topological_order()
    fingerprints = resolve_fingerprints(order)
    summary = RunSummary(run_id=run_id, db_path=store.db_path)

    sem = asyncio.Semaphore(max_workers) if max_workers else None
    pending: dict[str, asyncio.Task] = {}

    async def run_node(t: Task) -> None:
        for up in t.upstream_tasks():
            await pending[up.name]

        fp = fingerprints[t.name]
        upstream_names = [u.name for u in t.upstream_tasks()]

        # Lineage is recorded inside whichever transaction below actually
        # fires, not on its own -- so a DuckLake-backed store commits one
        # meaningfully-tagged snapshot per task, not lineage-plus-outcome
        # as two anonymous ones.

        failed_upstream = [
            up.name
            for up in t.upstream_tasks()
            if summary.statuses.get(up.name) in _FAILURE_STATUSES
        ]
        if failed_upstream:
            ts = now()
            summary.statuses[t.name] = "upstream_failed"
            summary.errors[t.name] = f"upstream task(s) failed: {', '.join(failed_upstream)}"
            # Recorded even though this task never ran, so `task_runs` --
            # and therefore resume on a later invocation, which relies on
            # this task having no recorded fingerprint/cache -- reflects
            # what actually happened (ROADMAP.md sec 11, "partial-DAG
            # resume... using the same fingerprint mechanism").
            with store.transaction(f"task {t.name} upstream_failed"):
                store.record_lineage(t.name, upstream_names)
                store.record_task_run(
                    run_id, t.name, "upstream_failed", ts, ts, fp, error=summary.errors[t.name]
                )
            logger.error("task %s skipped: %s", t.name, summary.errors[t.name])
            return

        # Skip-if-unchanged (tenet #6) only fires when there's a cached
        # value to skip *to* -- fingerprints are always recorded for
        # lineage/observability, but a task without cache=True has nothing
        # to hand downstream tasks without re-running.
        if not force and would_skip(store, t, fp):
            ts = now()
            summary.results[t.name] = store.get_cached(t.name, fp)
            summary.statuses[t.name] = "skipped"
            with store.transaction(f"task {t.name} skipped (unchanged)"):
                store.record_lineage(t.name, upstream_names)
                store.record_task_run(run_id, t.name, "skipped", ts, ts, fp)
            logger.info("task %s skipped (unchanged)", t.name)
            return

        kwargs = {pname: summary.results.get(up.name) for pname, up in t.upstream_params().items()}

        started = now()
        try:
            value = await _run_with_retries(t, kwargs)
        except Exception as exc:
            ended = now()
            summary.statuses[t.name] = "failed"
            summary.errors[t.name] = str(exc)
            with store.transaction(f"task {t.name} failed: {exc}"):
                store.record_lineage(t.name, upstream_names)
                store.record_task_run(run_id, t.name, "failed", started, ended, fp, error=str(exc))
            logger.error("task %s failed: %s", t.name, exc)
            return

        ended = now()
        summary.results[t.name] = value
        summary.statuses[t.name] = "success"
        # One transaction for the whole outcome: on a DuckLake-backed
        # store this is also what makes `commit_message` a coherent
        # per-task note in the snapshot history, not one entry per
        # low-level write (ROADMAP.md sec 8, Phase 3b).
        with store.transaction(f"task {t.name} succeeded"):
            store.record_lineage(t.name, upstream_names)
            store.record_task_run(run_id, t.name, "success", started, ended, fp)
            store.set_fingerprint(t.name, fp)
            if t.cache:
                try:
                    size = store.set_cached(t.name, fp, value, backend=t.cache_backend)
                except Exception as exc:
                    # The task itself already succeeded -- a caching failure
                    # only means skip-if-unchanged won't apply to it next
                    # run, not that the run failed. Common cause: the task
                    # returned a lazy, live-handle object (e.g. a
                    # DuckDBPyRelation) rather than a plain picklable value
                    # -- cache the materialized result instead, or leave
                    # cache=False and let the lazy plan rebuild cheaply on
                    # every run (ROADMAP.md sec 6.2/6.3).
                    logger.warning(
                        "task %s succeeded but its output could not be cached (%s: %s)",
                        t.name,
                        type(exc).__name__,
                        exc,
                    )
                else:
                    if size > LARGE_CACHE_WARN_BYTES:
                        logger.warning(
                            "task %s cached %.1fMB via %s -- consider a leaner cache_backend "
                            "or cache=False for large outputs",
                            t.name,
                            size / (1024 * 1024),
                            t.cache_backend,
                        )
        logger.info("task %s finished in %.1fms", t.name, (ended - started).total_seconds() * 1000)

    async def run_guarded(t: Task) -> None:
        if sem is not None:
            async with sem:
                await run_node(t)
        else:
            await run_node(t)

    for t in order:
        pending[t.name] = asyncio.create_task(run_guarded(t))
    await asyncio.gather(*pending.values())
    return summary


async def _execute_one(
    target: Task,
    fingerprints: dict[str, str],
    read_store: StateStore,
    write_store: StateStore,
    run_id: str,
    module_path: str,
    force: bool,
) -> RunSummary:
    """Run exactly one task (Phase 3a's ``only=``), reading upstream state
    from ``read_store`` and recording this task's own outcome to
    ``write_store`` -- the same object in local mode, a fresh scratch file
    destined to become a delta in distributed mode (see ``run()``).
    """
    summary = RunSummary(run_id=run_id, db_path=read_store.db_path)
    fp = fingerprints[target.name]
    upstream = target.upstream_tasks()

    for up in upstream:
        if read_store.get_fingerprint(up.name) != fingerprints[up.name]:
            raise UpstreamNotReadyError(
                f"{up.name!r} hasn't completed with its current code yet -- "
                f"dispatch it before {target.name!r}"
            )

    # start_run() manages its own transaction (see state.py) so it can be
    # called by many scoped workers sharing one run_id without nesting a
    # transaction inside a transaction here.
    write_store.start_run(module_path, run_id)
    upstream_names = [u.name for u in upstream]

    if not force and would_skip(read_store, target, fp):
        ts = now()
        summary.results[target.name] = read_store.get_cached(target.name, fp)
        summary.statuses[target.name] = "skipped"
        with write_store.transaction(f"task {target.name} skipped (unchanged)"):
            write_store.record_lineage(target.name, upstream_names)
            write_store.record_task_run(run_id, target.name, "skipped", ts, ts, fp)
        logger.info("task %s skipped (unchanged)", target.name)
        return summary

    kwargs: dict[str, Any] = {}
    for pname, up in target.upstream_params().items():
        if not up.cache:
            raise UpstreamNotCachedError(
                f"{target.name!r} depends on {up.name!r} for data, but "
                f"{up.name!r} has no cache=True -- a scoped (--only) run "
                f"can't get its output without a cached value to read"
            )
        kwargs[pname] = read_store.get_cached(up.name, fingerprints[up.name])

    started = now()
    try:
        value = await _run_with_retries(target, kwargs)
    except Exception as exc:
        ended = now()
        summary.statuses[target.name] = "failed"
        summary.errors[target.name] = str(exc)
        with write_store.transaction(f"task {target.name} failed: {exc}"):
            write_store.record_lineage(target.name, upstream_names)
            write_store.record_task_run(
                run_id, target.name, "failed", started, ended, fp, error=str(exc)
            )
        logger.error("task %s failed: %s", target.name, exc)
        return summary

    ended = now()
    summary.results[target.name] = value
    summary.statuses[target.name] = "success"
    with write_store.transaction(f"task {target.name} succeeded"):
        write_store.record_lineage(target.name, upstream_names)
        write_store.record_task_run(run_id, target.name, "success", started, ended, fp)
        write_store.set_fingerprint(target.name, fp)
        if target.cache:
            try:
                write_store.set_cached(target.name, fp, value, backend=target.cache_backend)
            except Exception as exc:
                logger.warning(
                    "task %s succeeded but its output could not be cached (%s: %s)",
                    target.name,
                    type(exc).__name__,
                    exc,
                )
    logger.info("task %s finished in %.1fms", target.name, (ended - started).total_seconds() * 1000)
    return summary


def _module_path(source: str | Path | ModuleType) -> str:
    if isinstance(source, ModuleType):
        return getattr(source, "__file__", None) or "<module>"
    return str(source)


def _default_db_path(source: str | Path | ModuleType) -> Path:
    path_str = _module_path(source)
    base = Path(path_str) if path_str != "<module>" else Path.cwd() / "pipeline.py"
    return base.parent / "duckpipe.db"


def _build_and_execute(
    source: str | Path | ModuleType,
    resolved_db_path: str | Path,
    force: bool,
    max_workers: int | None,
    *,
    data_path: str | None = None,
) -> RunSummary:
    dag = build_dag(source)
    with StateStore(resolved_db_path, data_path=data_path) as store:
        run_id = store.start_run(_module_path(source))
        try:
            summary = asyncio.run(_execute_dag(dag, store, run_id, force, max_workers))
            store.finish_run(run_id, "success" if summary.success else "failed")
        except Exception:
            store.finish_run(run_id, "failed")
            raise
    return summary


def _absorb_pending(state_uri: str, db_path: Path, *, delete: bool) -> None:
    from duckpipe.remote import absorb_pending

    with StateStore(db_path) as store:
        absorb_pending(state_uri, store.absorb_delta, delete=delete)


def _run_scoped(
    source: str | Path | ModuleType,
    resolved_db_path: Path,
    state_uri: str | None,
    only: str,
    force: bool,
    run_id: str | None,
) -> RunSummary:
    if state_uri:
        from duckpipe.remote import sync_down

        sync_down(state_uri, resolved_db_path)
        # A scoped run never re-uploads the canonical file, so it must
        # never delete the deltas it reads either -- see absorb_pending's
        # docstring for why deletion is only safe from a whole-run absorb.
        _absorb_pending(state_uri, resolved_db_path, delete=False)

    dag = build_dag(source)
    if only not in dag.tasks:
        raise ValueError(f"no task named {only!r} in {_module_path(source)}")

    order = dag.topological_order()
    fingerprints = resolve_fingerprints(order)
    target = dag.tasks[only]
    run_id = run_id or new_run_id()
    module_path = _module_path(source)

    # Local mode: read and write the one shared file directly, same as a
    # whole run -- there's only one process, one file, DuckDB's own OS
    # lock already covers it. Distributed mode: read the (freshly absorbed)
    # shared file, but write this task's own new rows to a fresh scratch
    # file instead -- that's what gets shipped as a delta, so this scoped
    # run never touches or uploads the big shared file at all.
    delta_path = resolved_db_path.with_suffix(".delta.duckdb") if state_uri else resolved_db_path
    if state_uri:
        delta_path.unlink(missing_ok=True)

    with StateStore(resolved_db_path) as read_store:
        if state_uri:
            with StateStore(delta_path) as write_store:
                summary = asyncio.run(
                    _execute_one(
                        target, fingerprints, read_store, write_store, run_id, module_path, force
                    )
                )
        else:
            summary = asyncio.run(
                _execute_one(
                    target, fingerprints, read_store, read_store, run_id, module_path, force
                )
            )

    if state_uri:
        from duckpipe.remote import write_delta

        write_delta(state_uri, delta_path)
        delta_path.unlink(missing_ok=True)

    return summary


def run(
    source: str | Path | ModuleType,
    *,
    db_path: str | Path | None = None,
    state_uri: str | None = None,
    force: bool = False,
    max_workers: int | None = None,
    lock: bool = True,
    only: str | None = None,
    run_id: str | None = None,
    data_path: str | None = None,
) -> RunSummary:
    """Run a pipeline end-to-end: build its DAG, execute it, record state.

    ``source`` is a path to a pipeline module, or an already-imported
    module. ``db_path`` defaults to ``duckpipe.db`` next to the pipeline
    file (ROADMAP.md tenet #4 -- zero-config, one file appears). Pass
    ``state_uri`` to sync that file to/from S3/GCS/Azure/local before and
    after the run (tenet #1, sec 9); requires the ``duckpipe[remote]``
    extra.

    When ``state_uri`` is set, an advisory lock (``duckpipe.remote.locked``)
    is held for the whole download-run-upload sequence, so two overlapping
    invocations against the same ``state_uri`` raise ``StateLockedError``
    instead of silently racing (ROADMAP.md sec 12, open question #5). Pass
    ``lock=False`` to opt back into the old unlocked behavior.

    Pass ``only=<task name>`` to run exactly that one task instead of the
    whole DAG (ROADMAP.md sec 8, Phase 3a) -- the primitive many stateless
    workers use to cooperate on one DAG. A scoped run needs its upstream
    tasks to have already completed with their current code (raises
    ``UpstreamNotReadyError`` otherwise) and, for any it needs data from,
    to have ``cache=True`` (raises ``UpstreamNotCachedError`` otherwise).
    Against a ``state_uri``, a scoped run never takes the whole-file lock:
    it writes only its own new rows to a uniquely-keyed delta file instead
    of re-uploading the whole state file, so many workers can each run
    ``--only`` concurrently, on different tasks or even the same one
    redundantly (fingerprint-based skip makes that harmless). Pass the same
    ``run_id`` to every scoped call in one distributed run so they group
    together once merged; omitted, each gets its own.

    Pass ``db_path="ducklake:sqlite:pipeline.ducklake.sqlite"`` (or any
    other DuckLake attach string) to opt into a DuckLake-backed state
    store instead of a plain file (ROADMAP.md sec 8, Phase 3b) -- real
    snapshot history over every run, with no other code change. ``data_path``
    overrides where DuckLake stores table data; omitted, it's derived as a
    sibling ``<catalog>.data/`` directory for a local sqlite/duckdb
    catalog, and required for anything else (e.g. a Postgres catalog).
    This backend is a local observability upgrade, deliberately not wired
    to ``state_uri``/``only`` -- both raise clearly if combined with it,
    rather than doing something ill-defined.
    """
    resolved_db_path: str | Path = db_path if db_path is not None else _default_db_path(source)

    if is_ducklake(resolved_db_path):
        if state_uri:
            raise DuckLakeCombinationError(
                "db_path='ducklake:...' can't be combined with state_uri -- DuckLake's "
                "own catalog handles multi-writer access differently than duckpipe's "
                "object-storage sync/lock mechanism (ROADMAP.md sec 8)"
            )
        if only:
            raise DuckLakeCombinationError(
                "db_path='ducklake:...' can't be combined with only= -- it's a local "
                "observability upgrade, deliberately separate from Phase 3a's "
                "distributed mechanism (ROADMAP.md sec 8)"
            )
        return _build_and_execute(source, resolved_db_path, force, max_workers, data_path=data_path)

    resolved_db_path = Path(resolved_db_path)

    if only is not None:
        return _run_scoped(source, resolved_db_path, state_uri, only, force, run_id)

    if not state_uri:
        return _build_and_execute(source, resolved_db_path, force, max_workers)

    from duckpipe.remote import locked, sync_down, sync_up

    with contextlib.ExitStack() as stack:
        if lock:
            stack.enter_context(locked(state_uri))
        sync_down(state_uri, resolved_db_path)
        # A whole-run is about to re-upload a fully merged file under the
        # lock, which is what makes it safe to also clean up the deltas
        # it just folded in (unlike a scoped run's read-only absorb).
        _absorb_pending(state_uri, resolved_db_path, delete=True)
        summary = _build_and_execute(source, resolved_db_path, force, max_workers)
        sync_up(state_uri, resolved_db_path)

    return summary
