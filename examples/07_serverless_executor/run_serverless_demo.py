"""Dispatches `pipeline.py`'s two tasks across two genuinely different
invocation shapes into the *same* distributed run (ROADMAP.md sec 8,
Phase 3c) -- proving the portability claim by mixing them, not just
running each in isolation:

- `extract` runs inside a container (`docker run duckpipe-worker ...`),
  invoked with argv the way any container platform (a bare box, ECS,
  Cloud Run, a Kubernetes Job) calls one.
- `summarize` runs through `handler.handler(event, context)`, invoked
  with an event dict the way a FaaS platform (Lambda, Modal, Cloud
  Functions) calls one -- no subprocess at all, just a function call.

Both dispatches are the exact same `duckpipe.run(module, only=task,
state_uri=...)` underneath (Phase 3a, unchanged) -- coordinating through
nothing but the shared `state_uri`, the same way `run_cluster.py` in
`../04_distributed_cluster` coordinates several worker *processes*. Swap
the `file://` bucket below for `s3://`/etc. and swap `docker run` for a
real ECS/Cloud Run invocation, and this becomes a genuine two-platform
production run with zero changes to `pipeline.py` or `handler.py`.

Build the image once, then run this:

    docker build -f examples/07_serverless_executor/Dockerfile -t duckpipe-worker .
    uv run python examples/07_serverless_executor/run_serverless_demo.py
"""

import shutil
import subprocess
import uuid
from pathlib import Path

from handler import handler

HERE = Path(__file__).parent
BUCKET = HERE / "serverless_bucket"
STATE_URI = f"file://{BUCKET / 'duckpipe.db'}"


def dispatch_via_container(task_name: str, run_id: str) -> None:
    print(f"  -> container worker for {task_name!r} (docker run)")
    result = subprocess.run(
        [
            "docker",
            "run",
            "--rm",
            "-v",
            f"{BUCKET}:/data",
            "duckpipe-worker",
            "--only",
            task_name,
            "--state-uri",
            "file:///data/duckpipe.db",
            "--run-id",
            run_id,
        ],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(f"container worker for {task_name!r} failed:\n{result.stderr}")


def dispatch_via_function(task_name: str, run_id: str) -> None:
    print(f"  -> function worker for {task_name!r} (handler(event, context))")
    result = handler({"task": task_name, "state_uri": STATE_URI, "run_id": run_id})
    if not result["success"]:
        raise RuntimeError(f"function worker for {task_name!r} failed: {result['errors']}")


def main() -> None:
    shutil.rmtree(BUCKET, ignore_errors=True)
    BUCKET.mkdir(parents=True)
    run_id = uuid.uuid4().hex

    print("dispatching one DAG across two invocation shapes, one state_uri:")
    dispatch_via_container("extract", run_id)
    dispatch_via_function("summarize", run_id)

    print("\ncompacting both workers' deltas into the canonical state file...")
    subprocess.run(["uv", "run", "duckpipe", "compact", STATE_URI], check=True)

    print("\nfinal state (queried straight from the compacted file):")
    result = subprocess.run(
        ["uv", "run", "duckpipe", "stats", str(BUCKET / "duckpipe.db")],
        capture_output=True,
        text=True,
        check=True,
    )
    print(result.stdout)


if __name__ == "__main__":
    main()
