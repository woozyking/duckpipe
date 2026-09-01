"""A minimal stand-in for what an Airflow/Prefect scheduler already does
natively -- proving the technique with plain `asyncio`, so it's checkable
without either installed. See this folder's README for the direct
translation to a real Airflow `pool=`/Prefect `concurrency()` recipe.

Discovery is `duckpipe show --json`, the same primitive
`examples/04_distributed_cluster/run_cluster.py` uses. Dispatch is
`duckpipe run --only <task>`, the same command any single-task worker
runs for itself. The difference from example 04: instead of waiting for
a whole dependency "level" to finish before starting the next, each task
is dispatched the moment its own dependencies are done -- gated by a
plain `asyncio.Semaphore` per named pool for tasks that have one
(mirroring Airflow pool slots / Prefect tagged concurrency limits
exactly), and by nothing at all for tasks that don't.

Like example 04, this needs ``--state-uri`` (even a local ``file://``
one), not a bare ``--db`` path -- confirmed the hard way, not assumed:
a first version of this script pointed several concurrent ``--only``
subprocesses at one shared ``--db`` file directly and reliably failed
with a write conflict the moment two independent tasks (``fast_a``/
``fast_b``/``fast_c``, all unlocked by ``extract`` at once) landed at
the same time. ``--state-uri``'s delta-merge mechanism (DESIGN.md sec 8)
exists precisely so concurrent scoped runs write their own uniquely-keyed
file instead of contending on one shared one -- a bare ``--db`` path
never gets that protection, distributed or not.
"""

import asyncio
import json
import shutil
import subprocess
import time
import uuid
from pathlib import Path

import pipeline as pl

HERE = Path(__file__).parent
PIPELINE = HERE / "pipeline.py"
BUCKET = HERE / "pools_bucket"
STATE_URI = f"file://{BUCKET / 'duckpipe.db'}"


def _discover() -> dict[str, set[str]]:
    result = subprocess.run(
        ["uv", "run", "duckpipe", "show", str(PIPELINE), "--json"],
        capture_output=True,
        text=True,
        check=True,
    )
    tasks = json.loads(result.stdout)
    return {t["task"]: set(t["depends_on"]) for t in tasks}


async def main() -> None:
    shutil.rmtree(BUCKET, ignore_errors=True)

    depends_on = _discover()
    pool_sems = {name: asyncio.Semaphore(cap) for name, cap in pl.POOL_CAPACITY.items()}
    done_events = {name: asyncio.Event() for name in depends_on}
    timings: dict[str, tuple[float, float]] = {}
    run_id = uuid.uuid4().hex

    async def dispatch(name: str) -> None:
        started = time.monotonic()
        scratch = HERE / f"_scratch_{name}_{uuid.uuid4().hex[:6]}.duckdb"
        proc = await asyncio.create_subprocess_exec(
            "uv",
            "run",
            "duckpipe",
            "run",
            str(PIPELINE),
            "--only",
            name,
            "--state-uri",
            STATE_URI,
            "--run-id",
            run_id,
            "--db",
            str(scratch),
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE,
        )
        _, stderr = await proc.communicate()
        if proc.returncode != 0:
            raise RuntimeError(f"worker for {name!r} failed: {stderr.decode()}")
        ended = time.monotonic()
        timings[name] = (started, ended)
        pool = pl.POOLS.get(name, "(none)")
        print(f"  {name:<12} pool={pool:<14} {started:6.2f}s -> {ended:6.2f}s", flush=True)

    async def run_one(name: str) -> None:
        for dep in depends_on[name]:
            await done_events[dep].wait()
        pool = pl.POOLS.get(name)
        if pool:
            async with pool_sems[pool]:
                await dispatch(name)
        else:
            await dispatch(name)
        done_events[name].set()

    t0 = time.monotonic()
    print(
        "dispatching (task, pool, start, end), relative to this script's own start:\n",
        flush=True,
    )
    await asyncio.gather(*(run_one(name) for name in depends_on))
    print(f"\ntotal: {time.monotonic() - t0:.2f}s", flush=True)

    a_start, a_end = timings["publish_a"]
    b_start, b_end = timings["publish_b"]
    overlapped = a_start < b_end and b_start < a_end
    print(
        f"publish_a / publish_b overlapped: {overlapped}  (pool capacity=1 -- must be False)",
        flush=True,
    )

    # fast_a/fast_b/fast_c share no pool at all -- if the external_api
    # pool's semaphore somehow throttled dispatch globally instead of
    # per-pool, these three would queue behind each other too, instead
    # of starting together the moment extract unlocks them.
    fast_starts = [timings[n][0] for n in ("fast_a", "fast_b", "fast_c")]
    fast_spread = max(fast_starts) - min(fast_starts)
    print(
        f"fast_a/fast_b/fast_c start spread: {fast_spread:.2f}s -- "
        "small means they ran together, unaffected by the external_api pool",
        flush=True,
    )

    assert not overlapped, "pool capacity violated -- publish_a/publish_b ran concurrently"

    print("\ncompacting worker deltas into the canonical state file...", flush=True)
    subprocess.run(["uv", "run", "duckpipe", "compact", STATE_URI], check=True)
    for scratch in HERE.glob("_scratch_*.duckdb"):
        scratch.unlink()
    for scratch in HERE.glob("_scratch_*.duckdb.wal"):
        scratch.unlink()


if __name__ == "__main__":
    asyncio.run(main())
