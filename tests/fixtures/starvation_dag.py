"""Fixture for the slot-starvation regression test (test_scheduler.py).

Named so the alphabetical tie-break in `DAG.topological_order` places
`combine` -- which depends on `slow_root` -- right after it, ahead of
the independent `zzz_independent` task. That ordering is exactly what
let a downstream task win a concurrency slot and idle in it while
waiting on its own upstream, starving an unrelated, ready-to-run task
out of a slot it could otherwise be using (see scheduler.py's own
comment on `run_guarded`).

Each task records its own start time (guarded by a lock -- these run in
real, concurrent OS threads) to a JSON file named by
`DUCKPIPE_TEST_TIMING_FILE`, so the test can compare *when* things
actually started rather than inferring it from total wall-clock time.
"""

import json
import os
import threading
import time
from pathlib import Path

from duckpipe import task

_TIMING_FILE = Path(os.environ["DUCKPIPE_TEST_TIMING_FILE"])
_LOCK = threading.Lock()


def _record(name: str) -> None:
    with _LOCK:
        times = json.loads(_TIMING_FILE.read_text()) if _TIMING_FILE.exists() else {}
        times[name] = time.monotonic()
        _TIMING_FILE.write_text(json.dumps(times))


@task
def slow_root():
    _record("slow_root")
    time.sleep(0.6)
    return "slow_root done"


@task
def combine(x=slow_root):
    _record("combine")
    return f"combined: {x}"


@task
def zzz_independent():
    _record("zzz_independent")
    return "z done"
