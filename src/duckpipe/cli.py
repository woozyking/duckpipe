"""``duckpipe`` command-line interface.

Every subcommand is a thin wrapper over the same public functions
(``duckpipe.run``, ``duckpipe.build_dag``) any other trigger -- cron, CI,
a Lambda handler -- would call directly (ROADMAP.md sec 5, sec 9).
"""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

import duckdb
import typer
from rich.console import Console
from rich.table import Table

from duckpipe.dag import build_dag
from duckpipe.scheduler import _default_db_path
from duckpipe.scheduler import run as run_pipeline

app = typer.Typer(
    name="duckpipe",
    help="A serverless-first, DuckDB-native pipeline orchestrator.",
    no_args_is_help=True,
)
console = Console()

_STATUS_STYLE = {"success": "green", "skipped": "yellow", "failed": "red"}


@app.command()
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
) -> None:
    """Run a pipeline module end-to-end."""
    summary = run_pipeline(
        pipeline,
        db_path=db,
        state_uri=state_uri,
        force=force,
        max_workers=max_workers,
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
def show(
    pipeline: Annotated[Path, typer.Argument(exists=True, help="Path to a pipeline .py module")],
    db: Annotated[
        Path | None, typer.Option("--db", help="State file to read last-run status from")
    ] = None,
) -> None:
    """Print the resolved DAG and each task's last-run status."""
    dag = build_dag(pipeline)
    order = dag.topological_order()
    db_path = db or _default_db_path(pipeline)

    last_status: dict[str, tuple[str, str]] = {}
    if db_path.exists():
        from duckpipe.state import StateStore

        with StateStore(db_path) as store:
            for t in order:
                row = store.last_status(t.name)
                if row:
                    last_status[t.name] = (row[0], str(row[1]))

    table = Table(title=f"DAG: {pipeline}")
    table.add_column("task")
    table.add_column("depends on")
    table.add_column("last status")
    table.add_column("last run")
    for t in order:
        deps = ", ".join(sorted(u.name for u in t.upstream_tasks())) or "-"
        status, ts = last_status.get(t.name, ("-", "-"))
        style = _STATUS_STYLE.get(status, "")
        rendered = f"[{style}]{status}[/{style}]" if style else status
        table.add_row(t.name, deps, rendered, ts)
    console.print(table)


@app.command()
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
        """
        SELECT task_name,
               count(*) AS runs,
               avg(duration_ms) AS avg_ms,
               max(duration_ms) AS max_ms,
               sum(CASE WHEN skipped THEN 1 ELSE 0 END) AS skipped
        FROM task_runs GROUP BY task_name ORDER BY avg_ms DESC
        """
    ).fetchall()
    stats_table = Table(title="per-task stats")
    for col in ("task", "runs", "avg_ms", "max_ms", "skipped"):
        stats_table.add_column(col)
    for row in task_stats:
        stats_table.add_row(*(f"{v:.1f}" if isinstance(v, float) else str(v) for v in row))
    console.print(stats_table)

    con.close()


def main() -> None:
    app()


if __name__ == "__main__":
    main()
