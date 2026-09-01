"""Bridging DuckPipe's deliberately single, global `max_workers` into a
host orchestrator's own *named* concurrency control (Airflow pools,
Prefect tagged concurrency limits) -- instead of DuckPipe growing a
resource-group concept of its own (DESIGN.md tenet #7: don't add a
concept when composition already covers the need).

Three tasks (`fast_a`/`fast_b`/`fast_c`) are ordinary parallel work. Two
more (`publish_a`/`publish_b`) simulate writing to a shared, rate-limited
external API -- they must never run at the same time as each other, but
have nothing to do with the fast_* tasks and shouldn't be held back by
them either. `max_workers=1` for the whole pipeline would serialize
*everything*; leaving it unbounded lets `publish_a`/`publish_b` race the
external resource. Neither is right.

``POOLS``/``POOL_CAPACITY`` below are the fix -- and deliberately not a
DuckPipe concept at all: two plain dicts a *coordinator* reads (see
``run_with_pools.py``), the same way DESIGN.md sec 8 already treats
"which worker runs which task, in what order" as something else's job.
Airflow calls this exact idea a pool (`pool=`); Prefect calls it a
tagged concurrency limit (`concurrency("name")`); this is the same
mapping either would want, expressed as nothing more than a dict,
because DuckPipe's own core has no reason to know it exists.

    uv run python examples/10_orchestrator_pools/run_with_pools.py
"""

import os
import time
from pathlib import Path

import duckdb

from duckpipe import task

HERE = Path(__file__).parent
DATA = os.environ.get(
    "DUCKPIPE_EXAMPLE_DATA", str(HERE.parent / "data" / "nyc_taxi_sample.parquet")
)


@task(cache=True)
def extract():
    return duckdb.sql(f"SELECT * FROM read_parquet('{DATA}')").pl()


@task(cache=True)
def fast_a(trips=extract):
    time.sleep(0.3)
    return trips.filter(trips["payment_type"] == 1).height


@task(cache=True)
def fast_b(trips=extract):
    time.sleep(0.3)
    return trips.filter(trips["payment_type"] == 2).height


@task(cache=True)
def fast_c(trips=extract):
    time.sleep(0.3)
    return trips["trip_distance"].mean()


@task(cache=True)
def publish_a(count=fast_a):
    time.sleep(1.0)  # stands in for a real rate-limited API call
    return {"published": "a", "rows": count}


@task(cache=True)
def publish_b(count=fast_b):
    time.sleep(1.0)
    return {"published": "b", "rows": count}


# The bridge itself: a plain mapping from task name to a named pool, and
# each pool's own capacity -- nothing DuckPipe's own API knows about.
# A task absent from POOLS is unconstrained, same as today's default.
POOLS: dict[str, str] = {
    "publish_a": "external_api",
    "publish_b": "external_api",
}
POOL_CAPACITY: dict[str, int] = {
    "external_api": 1,
}


if __name__ == "__main__":
    from duckpipe import run

    run(__file__, db_path=HERE / "duckpipe.db")
