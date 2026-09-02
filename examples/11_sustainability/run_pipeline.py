"""The one invocation shape this whole comparison rests on: a plain
function, callable however a scheduler wants to call it -- cron,
EventBridge, a Kubernetes CronJob, a Prefect push work pool submitting
straight to AWS Fargate or Google Cloud Run, anything. Deliberately not
shaped like any one platform's own calling convention (compare
`examples/07_serverless_executor/handler.py`, which *is* AWS Lambda's
`(event, context)` shape on purpose, to prove DuckPipe isn't locked to
one invocation style). This one runs the *entire* pipeline in one call,
not one `--only` task -- the realistic shape for a modest daily batch
job: one scheduled invocation, start to finish, nothing else involved.

This exact function is what both arms of the sustainability comparison
call. Identically. In the standing-orchestrator arm, a task doesn't run
this code in-process on a worker -- it triggers this same function
somewhere and waits on it. The only thing that differs between the two
arms is what, if anything, has to be running continuously *before* this
function is called -- see this folder's README, including what "worker"
means once it isn't the thing actually executing the pipeline.
"""

from pathlib import Path
from typing import Any

from duckpipe import run

PIPELINE = Path(__file__).parent / "pipeline.py"


def run_pipeline(db_path: str | None = None) -> dict[str, Any]:
    db_path = db_path or str(Path("/tmp") / "duckpipe.db")
    summary = run(PIPELINE, db_path=db_path)
    # Only `report`'s own value -- `summary.results` holds every task's
    # return value for this run, including `extract`/`clean`/
    # `join_boroughs`'s live, lazy DuckDBPyRelation objects (never
    # `cache=True`, per pipeline.py's own docstring). Those aren't
    # picklable across a process boundary; a real invocation's own
    # response payload wouldn't try to serialize them either.
    return {
        "success": summary.success,
        "statuses": summary.statuses,
        "report": summary.results.get("report"),
    }
