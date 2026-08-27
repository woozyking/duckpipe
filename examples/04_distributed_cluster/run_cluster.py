"""The coordinator role DESIGN.md sec 8 deliberately keeps outside
DuckPipe: dispatch `pipeline.py`'s tasks to several worker *processes*
sharing state only through a `file://` "bucket" -- swap that for a real
s3://... URI and nothing else about this script changes to run on an
actual cluster.

Discovery uses `duckpipe show --json` (topological order + dependencies);
dispatch is `duckpipe run --only <task> --state-uri ... --run-id ...`, the
same command any single-task worker would run for itself. Tasks with no
unmet dependencies are dispatched together as real, concurrent OS
processes; the next "level" waits for the previous one. A final
`duckpipe compact` folds every worker's delta into the canonical state
file (see `absorb_pending`'s docstring in `duckpipe.remote` for why
scoped runs never do that themselves).

Run: `uv run python examples/04_distributed_cluster/run_cluster.py`
"""

import json
import shutil
import subprocess
import uuid
from pathlib import Path

HERE = Path(__file__).parent
PIPELINE = HERE / "pipeline.py"
BUCKET = HERE / "cluster_bucket"
STATE_URI = f"file://{BUCKET / 'duckpipe.db'}"


def duckpipe(*args: str, check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["uv", "run", "duckpipe", *args], capture_output=True, text=True, check=check
    )


def dispatch(task_name: str, run_id: str) -> subprocess.Popen:
    scratch = HERE / f"_scratch_{task_name}_{uuid.uuid4().hex[:6]}.duckdb"
    print(f"  -> worker for {task_name!r}")
    return subprocess.Popen(
        [
            "uv",
            "run",
            "duckpipe",
            "run",
            str(PIPELINE),
            "--only",
            task_name,
            "--state-uri",
            STATE_URI,
            "--run-id",
            run_id,
            "--db",
            str(scratch),
        ]
    )


def main() -> None:
    shutil.rmtree(BUCKET, ignore_errors=True)

    tasks = json.loads(duckpipe("show", str(PIPELINE), "--json").stdout)
    depends_on = {t["task"]: set(t["depends_on"]) for t in tasks}

    run_id = uuid.uuid4().hex
    done: set[str] = set()
    level = 1
    while len(done) < len(tasks):
        ready = sorted(
            name for name, deps in depends_on.items() if name not in done and deps <= done
        )
        print(f"level {level}: dispatching {ready} to {len(ready)} worker(s) in parallel")
        procs = [dispatch(name, run_id) for name in ready]
        for p, name in zip(procs, ready, strict=True):
            if p.wait() != 0:
                raise RuntimeError(f"worker for {name!r} exited with code {p.returncode}")
        done |= set(ready)
        level += 1

    print("\ncompacting worker deltas into the canonical state file...")
    duckpipe("compact", STATE_URI)

    print("\nfinal state (queried straight from the compacted file):")
    result = duckpipe("stats", str(BUCKET / "duckpipe.db"))
    print(result.stdout)

    for scratch in HERE.glob("_scratch_*.duckdb"):
        scratch.unlink()


if __name__ == "__main__":
    main()
