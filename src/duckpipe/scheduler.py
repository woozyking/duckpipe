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
import logging
from dataclasses import dataclass, field
from pathlib import Path
from types import ModuleType
from typing import Any, Literal

from duckpipe.dag import DAG, build_dag
from duckpipe.fingerprint import resolve_fingerprints
from duckpipe.state import LARGE_CACHE_WARN_BYTES, StateStore, now
from duckpipe.task import Task

logger = logging.getLogger("duckpipe")

Status = Literal["success", "skipped", "failed", "upstream_failed"]
_FAILURE_STATUSES = ("failed", "upstream_failed")


@dataclass
class RunSummary:
    run_id: str
    db_path: Path
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
        store.record_lineage(t.name, [u.name for u in t.upstream_tasks()])

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
            store.record_task_run(run_id, t.name, "failed", started, ended, fp, error=str(exc))
            logger.error("task %s failed: %s", t.name, exc)
            return

        ended = now()
        summary.results[t.name] = value
        summary.statuses[t.name] = "success"
        store.record_task_run(run_id, t.name, "success", started, ended, fp)
        store.set_fingerprint(t.name, fp)
        if t.cache:
            try:
                size = store.set_cached(t.name, fp, value, backend=t.cache_backend)
            except Exception as exc:
                # The task itself already succeeded -- a caching failure only
                # means skip-if-unchanged won't apply to it next run, not that
                # the run failed. Common cause: the task returned a lazy,
                # live-handle object (e.g. a DuckDBPyRelation) rather than a
                # plain picklable value -- cache the materialized result
                # instead, or leave cache=False and let the lazy plan rebuild
                # cheaply on every run (ROADMAP.md sec 6.2/6.3).
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


def _module_path(source: str | Path | ModuleType) -> str:
    if isinstance(source, ModuleType):
        return getattr(source, "__file__", None) or "<module>"
    return str(source)


def _default_db_path(source: str | Path | ModuleType) -> Path:
    path_str = _module_path(source)
    base = Path(path_str) if path_str != "<module>" else Path.cwd() / "pipeline.py"
    return base.parent / "duckpipe.db"


def run(
    source: str | Path | ModuleType,
    *,
    db_path: str | Path | None = None,
    state_uri: str | None = None,
    force: bool = False,
    max_workers: int | None = None,
) -> RunSummary:
    """Run a pipeline end-to-end: build its DAG, execute it, record state.

    ``source`` is a path to a pipeline module, or an already-imported
    module. ``db_path`` defaults to ``duckpipe.db`` next to the pipeline
    file (ROADMAP.md tenet #4 -- zero-config, one file appears). Pass
    ``state_uri`` to sync that file to/from S3/GCS/Azure/local before and
    after the run (tenet #1, sec 9); requires the ``duckpipe[remote]``
    extra.
    """
    resolved_db_path = Path(db_path) if db_path is not None else _default_db_path(source)

    if state_uri:
        from duckpipe.remote import sync_down

        sync_down(state_uri, resolved_db_path)

    dag = build_dag(source)

    with StateStore(resolved_db_path) as store:
        run_id = store.start_run(_module_path(source))
        try:
            summary = asyncio.run(_execute_dag(dag, store, run_id, force, max_workers))
            store.finish_run(run_id, "success" if summary.success else "failed")
        except Exception:
            store.finish_run(run_id, "failed")
            raise

    if state_uri:
        from duckpipe.remote import sync_up

        sync_up(state_uri, resolved_db_path)

    return summary
