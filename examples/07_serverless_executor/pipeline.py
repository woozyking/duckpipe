"""The DAG this example dispatches across two genuinely different
invocation shapes (DESIGN.md sec 8) -- a container and a
function-as-a-service handler -- to check, not just claim, that
`duckpipe.run(module, only=task, state_uri=...)` really is a plain
Python call with no platform-specific glue baked in.

    extract -> summarize

Deliberately duckdb-only (no polars/pandas/pyarrow): the point of this
example is the *invocation shape*, not the data engine, so the container
image built from it stays small and the handler needs no extra
dependency either. Both tasks return plain, always-picklable Python
values (a list of tuples, a dict) -- no `cache_backend="arrow"` needed
for a `--only` worker to hand its output to the next one.
"""

import os
from pathlib import Path

import duckdb

from duckpipe import task

HERE = Path(__file__).parent
# See examples/01_daily_batch_etl/duck.py's comment on this override.
DATA = os.environ.get(
    "DUCKPIPE_EXAMPLE_DATA", str(HERE.parent / "data" / "nyc_taxi_sample.parquet")
)


@task(cache=True)
def extract():
    """The task the container-shaped worker runs."""
    return duckdb.sql(
        f"SELECT payment_type, count(*) AS trips, round(sum(fare_amount), 2) AS total_fare "
        f"FROM read_parquet('{DATA}') GROUP BY 1 ORDER BY 1"
    ).fetchall()


@task(cache=True)
def summarize(rows=extract):
    """The task the function-shaped worker runs -- reads `extract`'s
    cached value exactly the way any `--only` worker does, regardless of
    what process or runtime dispatched it.
    """
    return {"payment_types": len(rows), "total_fare": round(sum(r[2] for r in rows), 2)}


if __name__ == "__main__":
    from duckpipe import run

    run(__file__, db_path=HERE / "duckpipe.db")
