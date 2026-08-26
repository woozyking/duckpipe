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

from duckpipe.dag import DAG, CycleError, build_dag
from duckpipe.remote import StateLockedError
from duckpipe.scheduler import RunSummary, run
from duckpipe.task import Task, task

__all__ = [
    "task",
    "run",
    "build_dag",
    "Task",
    "DAG",
    "CycleError",
    "RunSummary",
    "StateLockedError",
]

__version__ = "0.1.0"
