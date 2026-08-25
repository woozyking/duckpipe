"""``duckpipe`` command-line interface.

Every subcommand is a thin wrapper over the same public functions
(``duckpipe.run``, ``duckpipe.build_dag``) any other trigger -- cron, CI,
a Lambda handler -- would call directly (ROADMAP.md sec 5, sec 9).
"""

from __future__ import annotations

import functools
from collections.abc import Callable
from pathlib import Path
from typing import Annotated

import duckdb
import typer
from rich.console import Console
from rich.table import Table

from duckpipe.dag import CycleError, DuplicateTaskNameError, build_dag
from duckpipe.fingerprint import resolve_fingerprints
from duckpipe.remote import StateLockedError
from duckpipe.scheduler import (
    UpstreamNotCachedError,
    UpstreamNotReadyError,
    _default_db_path,
    would_skip,
)
from duckpipe.scheduler import run as run_pipeline

app = typer.Typer(
    name="duckpipe",
    help="A serverless-first, DuckDB-native pipeline orchestrator.",
    no_args_is_help=True,
    pretty_exceptions_enable=False,
)
console = Console()

_STATUS_STYLE = {"success": "green", "skipped": "yellow", "failed": "red"}

# A malformed pipeline (a cycle, a duplicate task name, a plain typo that
# blows up at import time), a locked state_uri, or a scoped (--only) run
# dispatched out of order is a user-actionable situation, not a DuckPipe
# bug -- it should read as one short line, not a wall of framework frames.
_USER_FACING_ERRORS = (
    CycleError,
    DuplicateTaskNameError,
    ImportError,
    SyntaxError,
    StateLockedError,
    UpstreamNotReadyError,
    UpstreamNotCachedError,
    ValueError,
)


def _friendly_errors[F: Callable[..., object]](command: F) -> F:
    @functools.wraps(command)
    def wrapper(*args: object, **kwargs: object) -> object:
        try:
            return command(*args, **kwargs)
        except typer.Exit:
            raise
        except _USER_FACING_ERRORS as exc:
            console.print(f"[red]error:[/red] {exc}")
            raise typer.Exit(code=1) from None
        except Exception as exc:  # noqa: BLE001 - last-resort CLI-friendly fallback
            console.print(f"[red]error:[/red] {type(exc).__name__}: {exc}")
            raise typer.Exit(code=1) from None

    return wrapper


@app.command()
@_friendly_errors
def run(
    pipeline: Annotated[Path, typer.Argument(exists=True, help="Path to a pipeline .py module")],
    db: Annotated[
        Path | None,
        typer.Option("--db", help="State file path (default: duckpipe.db next to the pipeline)"),
    ] = None,
    state_uri: Annotated[
        str | None,
        typer.Option("--state-uri", help="fsspec URI to sync state to/from (S3/GCS/Azure/local)"),
    ] = None,
    force: Annotated[
        bool, typer.Option("--force", help="Ignore fingerprints; re-run every task")
    ] = False,
    max_workers: Annotated[
        int | None, typer.Option("--max-workers", help="Cap concurrent tasks")
    ] = None,
    no_lock: Annotated[
        bool,
        typer.Option(
            "--no-lock",
            help="With --state-uri, skip the advisory lock and allow racing (not recommended)",
        ),
    ] = False,
    only: Annotated[
        str | None,
        typer.Option("--only", help="Run just this one task -- for distributed dispatch"),
    ] = None,
    run_id: Annotated[
        str | None,
        typer.Option("--run-id", help="Shared run id so --only calls for one run group together"),
    ] = None,
) -> None:
    """Run a pipeline module end-to-end, or (with --only) exactly one task."""
    summary = run_pipeline(
        pipeline,
        db_path=db,
        state_uri=state_uri,
        force=force,
        max_workers=max_workers,
        lock=not no_lock,
        only=only,
        run_id=run_id,
    )
    table = Table(title=f"run {summary.run_id}  ({summary.db_path})")
    table.add_column("task")
    table.add_column("status")
    table.add_column("detail")
    for name, status in summary.statuses.items():
        style = _STATUS_STYLE.get(status, "")
        rendered = f"[{style}]{status}[/{style}]" if style else status
        table.add_row(name, rendered, summary.errors.get(name, ""))
    console.print(table)
    if not summary.success:
        raise typer.Exit(code=1)


@app.command()
@_friendly_errors
def show(
    pipeline: Annotated[Path, typer.Argument(exists=True, help="Path to a pipeline .py module")],
    db: Annotated[
        Path | None, typer.Option("--db", help="State file to read last-run status from")
    ] = None,
    as_json: Annotated[
        bool,
        typer.Option(
            "--json", help="Machine-readable output -- topological order + next-run preview"
        ),
    ] = False,
) -> None:
    """Print the resolved DAG, each task's last-run status, and what would
    happen if you ran it again right now -- a dry-run preview of the same
    fingerprint check `run` itself uses, so you can see what's stale
    before spending the time to re-run it. `--json` is the discovery
    primitive a coordinator dispatching `--only <task>` calls needs
    (ROADMAP.md sec 8): topological order plus which tasks would skip."""
    dag = build_dag(pipeline)
    order = dag.topological_order()
    fingerprints = resolve_fingerprints(order)
    db_path = db or _default_db_path(pipeline)

    last_status: dict[str, tuple[str, str]] = {}
    next_run: dict[str, str] = {}
    if db_path.exists():
        from duckpipe.state import StateStore

        with StateStore(db_path) as store:
            for t in order:
                row = store.last_status(t.name)
                if row:
                    last_status[t.name] = (row[0], str(row[1]))
                if would_skip(store, t, fingerprints[t.name]):
                    next_run[t.name] = "skip (unchanged)"
                elif not t.cache:
                    next_run[t.name] = "run (not cached)"
                else:
                    next_run[t.name] = "run (changed)"
    else:
        next_run = {t.name: "run (no prior state)" for t in order}

    if as_json:
        import json

        print(
            json.dumps(
                [
                    {
                        "task": t.name,
                        "depends_on": sorted(u.name for u in t.upstream_tasks()),
                        "next_run": next_run[t.name],
                    }
                    for t in order
                ]
            )
        )
        return

    table = Table(title=f"DAG: {pipeline}")
    table.add_column("task")
    table.add_column("depends on")
    table.add_column("last status")
    table.add_column("last run")
    table.add_column("next run")
    for t in order:
        deps = ", ".join(sorted(u.name for u in t.upstream_tasks())) or "-"
        status, ts = last_status.get(t.name, ("-", "-"))
        style = _STATUS_STYLE.get(status, "")
        rendered = f"[{style}]{status}[/{style}]" if style else status
        next_style = "yellow" if next_run[t.name].startswith("skip") else ""
        next_rendered = (
            f"[{next_style}]{next_run[t.name]}[/{next_style}]" if next_style else next_run[t.name]
        )
        table.add_row(t.name, deps, rendered, ts, next_rendered)
    console.print(table)


@app.command()
@_friendly_errors
def compact(
    state_uri: Annotated[str, typer.Argument(help="fsspec URI of the state file to compact")],
    no_lock: Annotated[
        bool, typer.Option("--no-lock", help="Skip the advisory lock (not recommended)")
    ] = False,
) -> None:
    """Fold pending per-task deltas from `--only` runs into the canonical
    state file, and clean them up. Nothing needs this for correctness --
    every invocation already absorbs pending deltas itself -- but a purely
    distributed workflow (many `--only` workers, no whole-run ever) never
    otherwise re-uploads the canonical file, so its `.pending/` directory
    only grows. Run this periodically (e.g. from cron) if that's your
    workflow."""
    from duckpipe.remote import compact as compact_state

    compact_state(state_uri, lock=not no_lock)
    console.print(f"[green]compacted[/green] {state_uri}")


@app.command()
@_friendly_errors
def stats(
    db: Annotated[Path, typer.Argument(exists=True, help="Path to a duckpipe.db state file")],
    limit: Annotated[int, typer.Option(help="Number of recent runs to show")] = 20,
) -> None:
    """Show recent pipeline runs and per-task duration stats from the state file."""
    con = duckdb.connect(str(db), read_only=True)

    runs = con.execute(
        "SELECT run_id, module_path, started_at, ended_at, status "
        "FROM pipeline_runs ORDER BY started_at DESC LIMIT ?",
        [limit],
    ).fetchall()
    runs_table = Table(title="recent pipeline runs")
    for col in ("run_id", "module", "started_at", "ended_at", "status"):
        runs_table.add_column(col)
    for row in runs:
        runs_table.add_row(*(str(v) for v in row))
    console.print(runs_table)

    task_stats = con.execute(
        "SELECT task_name, runs, avg_duration_ms, max_duration_ms, skipped_count, failed_count "
        "FROM v_task_stats ORDER BY avg_duration_ms DESC"
    ).fetchall()
    stats_table = Table(title="per-task stats (from v_task_stats)")
    for col in ("task", "runs", "avg_ms", "max_ms", "skipped", "failed"):
        stats_table.add_column(col)
    for row in task_stats:
        stats_table.add_row(*(f"{v:.1f}" if isinstance(v, float) else str(v) for v in row))
    console.print(stats_table)

    con.close()


def main() -> None:
    app()


if __name__ == "__main__":
    main()
