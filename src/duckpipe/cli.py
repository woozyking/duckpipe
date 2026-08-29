"""``duckpipe`` command-line interface.

Every subcommand is a thin wrapper over the same public functions
(``duckpipe.run``, ``duckpipe.build_dag``) any other trigger -- cron, CI,
a Lambda handler -- would call directly (DESIGN.md sec 5, sec 9).
"""

from __future__ import annotations

import functools
from collections.abc import Callable
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console
from rich.table import Table

from duckpipe.dag import CycleError, DuplicateTaskNameError, build_dag, to_json, to_mermaid
from duckpipe.fingerprint import resolve_fingerprints
from duckpipe.remote import StateLockedError
from duckpipe.scheduler import (
    UpstreamNotCachedError,
    UpstreamNotReadyError,
    _default_db_path,
    would_skip,
)
from duckpipe.scheduler import run as run_pipeline
from duckpipe.state import is_ducklake

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

_DUCKLAKE_FILE_PREFIXES = ("ducklake:sqlite:", "ducklake:duckdb:")


def _state_probably_exists(db_path: str | Path) -> bool:
    """A cheap existence check that never opens (and so never creates) the
    store, for commands like `show`/`stats` that shouldn't conjure a
    fresh, empty state file just by asking about one that isn't there yet.
    """
    if is_ducklake(db_path):
        for prefix in _DUCKLAKE_FILE_PREFIXES:
            if db_path.startswith(prefix):
                return Path(db_path[len(prefix) :]).exists()
        return True  # a live catalog (e.g. Postgres) -- can't check cheaply
    return Path(db_path).exists()


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
        str | None,
        typer.Option(
            "--db",
            help="State file path, or a ducklake:... catalog "
            "(default: duckpipe.db next to the pipeline)",
        ),
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
    data_path: Annotated[
        str | None,
        typer.Option(
            "--data-path", help="DuckLake DATA_PATH (only with --db ducklake:...; usually auto)"
        ),
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
        data_path=data_path,
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
        str | None, typer.Option("--db", help="State file to read last-run status from")
    ] = None,
    as_json: Annotated[
        bool,
        typer.Option(
            "--json", help="Machine-readable output -- topological order + next-run preview"
        ),
    ] = False,
    mermaid: Annotated[
        bool,
        typer.Option(
            "--mermaid", help="Print a Mermaid flowchart of the DAG, colored by last status"
        ),
    ] = False,
) -> None:
    """Print the resolved DAG, each task's last-run status, and what would
    happen if you ran it again right now -- a dry-run preview of the same
    fingerprint check `run` itself uses, so you can see what's stale
    before spending the time to re-run it. `--json` is the discovery
    primitive a coordinator dispatching `--only <task>` calls needs
    (DESIGN.md sec 8): topological order plus which tasks would skip.
    `--mermaid` prints a flowchart instead -- paste it into any Markdown
    that renders Mermaid."""
    if as_json and mermaid:
        raise ValueError("--json and --mermaid are two different output modes -- pick one")
    dag = build_dag(pipeline)
    order = dag.topological_order()
    fingerprints = resolve_fingerprints(order)
    db_path: str | Path = db or _default_db_path(pipeline)

    last_status: dict[str, tuple[str, str]] = {}
    next_run: dict[str, str] = {}
    if _state_probably_exists(db_path):
        from duckpipe.state import StateStore

        # read_only: `show` never needs to write, and must never block on
        # (or be blocked by) a pipeline that's still running.
        with StateStore(db_path, read_only=True) as store:
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

    if mermaid:
        print(to_mermaid(order, last_status))
        return

    if as_json:
        import json

        print(json.dumps(to_json(order, next_run)))
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
    db: Annotated[str, typer.Argument(help="Path to a state file, or a ducklake:... catalog")],
    limit: Annotated[int, typer.Option(help="Number of recent runs to show")] = 20,
    snapshots: Annotated[
        bool,
        typer.Option(
            "--snapshots", help="Show DuckLake snapshot history (time travel) instead of stats"
        ),
    ] = False,
) -> None:
    """Show recent pipeline runs and per-task duration stats from the state
    file. Against a DuckLake-backed store (``--db ducklake:...``), pass
    ``--snapshots`` to see every commit instead -- the time-travel-over-
    run-history payoff of that backend (DESIGN.md sec 8, Phase 3b)."""
    from duckpipe.state import StateStore

    # read_only: never blocks on, or is blocked by, a pipeline still
    # running against the same state file.
    with StateStore(db, read_only=True) as store:
        if snapshots:
            _print_snapshots(store)
            return

        runs = store.con.execute(
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

        task_stats = store.con.execute(
            "SELECT task_name, runs, avg_duration_ms, max_duration_ms, skipped_count, failed_count "
            "FROM v_task_stats ORDER BY avg_duration_ms DESC"
        ).fetchall()
        stats_table = Table(title="per-task stats (from v_task_stats)")
        for col in ("task", "runs", "avg_ms", "max_ms", "skipped", "failed"):
            stats_table.add_column(col)
        for row in task_stats:
            stats_table.add_row(*(f"{v:.1f}" if isinstance(v, float) else str(v) for v in row))
        console.print(stats_table)

        if store.is_ducklake:
            console.print("[dim]DuckLake-backed -- see full history with --snapshots[/dim]")


def _print_snapshots(store) -> None:
    columns = ("snapshot_id", "snapshot_time", "author", "commit_message")
    table = Table(title="snapshot history (time travel)")
    for col in columns:
        table.add_column(col)
    for snap in store.snapshots():
        table.add_row(*(str(snap[col]) if snap[col] is not None else "-" for col in columns))
    console.print(table)
    console.print(
        "[dim]query any table AT (VERSION => snapshot_id) to see state as of that point[/dim]"
    )


def main() -> None:
    app()


if __name__ == "__main__":
    main()
