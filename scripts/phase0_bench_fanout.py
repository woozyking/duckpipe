"""Phase 0 spike deliverable (ROADMAP.md sec 11):

    "Confirm cursor-sharing concurrency pattern behaves as expected under
    DAG-level fan-out with DuckDB's GIL-release behavior (benchmark: does
    concurrent-cursor fan-out actually beat serial execution on a synthetic
    CPU-bound + I/O-bound task mix?)."

This builds a fan-out DAG of independent tasks -- some CPU-bound *inside*
DuckDB (a heavy aggregation, which releases the GIL per the documented
behavior in ROADMAP.md sec 7), some I/O-bound (a sleep standing in for a
network call) -- all sharing one DuckDB connection via `con.cursor()`
(the documented-safe concurrency pattern), and compares running them
through DuckPipe's scheduler against running the same functions serially.

Run: `uv run python scripts/phase0_bench_fanout.py`
"""

from __future__ import annotations

import time

import duckdb

from duckpipe import run, task

N_DUCKDB_TASKS = 4
N_SLEEP_TASKS = 4
DUCKDB_ROWS = 30_000_000
SLEEP_SECONDS = 0.4

_shared_con = duckdb.connect(":memory:")

duckdb_tasks = []
for i in range(N_DUCKDB_TASKS):

    @task(name=f"duckdb_agg_{i}")
    def duckdb_agg(bucket=i):
        cur = _shared_con.cursor()
        row = cur.execute(
            f"SELECT count(*), sum(x) FROM range({DUCKDB_ROWS}) t(x) WHERE x % 7 = {bucket}"
        ).fetchone()
        cur.close()
        return row

    duckdb_tasks.append(duckdb_agg)

sleep_tasks = []
for i in range(N_SLEEP_TASKS):

    @task(name=f"io_wait_{i}")
    def io_wait():
        time.sleep(SLEEP_SECONDS)
        return "done"

    sleep_tasks.append(io_wait)

all_tasks = duckdb_tasks + sleep_tasks


def run_serial() -> float:
    start = time.perf_counter()
    for t in all_tasks:
        t()
    return time.perf_counter() - start


def run_concurrent() -> float:
    from types import ModuleType

    module = ModuleType("bench_fanout_module")
    for t in all_tasks:
        setattr(module, t.name, t)

    start = time.perf_counter()
    summary = run(module, db_path="/tmp/duckpipe_bench.duckdb", force=True)
    elapsed = time.perf_counter() - start
    assert summary.success, summary.errors
    return elapsed


if __name__ == "__main__":
    print(f"tasks: {N_DUCKDB_TASKS} DuckDB aggregations + {N_SLEEP_TASKS} sleeps, all independent")

    serial_s = run_serial()
    print(f"serial:     {serial_s:.2f}s")

    concurrent_s = run_concurrent()
    print(f"concurrent: {concurrent_s:.2f}s")

    speedup = serial_s / concurrent_s
    print(f"speedup:    {speedup:.2f}x")
    assert speedup > 1.3, "expected concurrent cursor fan-out to meaningfully beat serial execution"
    print("PASS: concurrent-cursor fan-out beats serial execution")
