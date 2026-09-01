"""DuckPipe: a serverless-first, DuckDB-native pipeline orchestrator.

No scheduler daemon, no central metadata database required, no broker
-- a run is a process that starts, does work, records what it did to a
``.duckdb`` file, and exits::

    from duckpipe import task, run

    @task
    def extract():
        return duckdb.sql("select * from read_parquet('data.parquet')")

    @task(cache=True)
    def transform(rel=extract):  # `rel=extract` infers the dependency
        return rel.filter("amount > 0")

    if __name__ == "__main__":
        run(__file__)

Then from a shell: ``duckpipe run pipeline.py``.
"""

from __future__ import annotations

from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as _version

from duckpipe.dag import DAG, CycleError, build_dag, to_json, to_mermaid
from duckpipe.remote import StateLockedError
from duckpipe.scheduler import RunSummary, run
from duckpipe.task import Task, task

__all__ = [
    "task",
    "run",
    "build_dag",
    "to_mermaid",
    "to_json",
    "Task",
    "DAG",
    "CycleError",
    "RunSummary",
    "StateLockedError",
]

# Read from the installed package's own metadata rather than duplicating
# pyproject.toml's version as a second literal -- a hardcoded string here
# already drifted out of sync with a real release once (0.4.0 published
# to PyPI while this file still said "0.3.0"), and a single source of
# truth is what actually fixes that, not remembering to update two
# places every time. Falls back when there's genuinely no installed
# package to ask -- confirmed the hard way: examples/08_browser_wasm
# copies this file's own raw source straight into a Pyodide sandbox
# (prepare_bundle.py, DESIGN.md sec 8) with no wheel/dist-info at all,
# and `import duckpipe` failing outright there is worse than an
# admittedly-unhelpful version string.
try:
    __version__ = _version("duckpipe")
except PackageNotFoundError:
    __version__ = "0.0.0+unknown"
