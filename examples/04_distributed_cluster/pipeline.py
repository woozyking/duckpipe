"""A DAG built to be dispatched across worker *processes*, not run in one
process, using the `only=`/`--only` mechanism and delta-merge state
(DESIGN.md sec 8) -- no DuckLake, no Quack, no new infrastructure.

    extract -> {by_payment_type, by_hour} -> combined_report

Unlike examples 01/02 (lazy end to end, nothing worth caching), every
task here needs `cache=True`. That's a second, distinct reason to
materialize, beyond example 03's "this step is expensive": a `--only`
worker can only receive its input as an already-cached *value* -- it
never re-derives an upstream task itself, since re-deriving would defeat
the point of dispatching work across machines in the first place.

See `run_cluster.py` in this folder for the actual distributed run, or
run any one task directly, the same way a real worker would:

    uv run duckpipe run examples/04_distributed_cluster/pipeline.py \\
        --only extract --state-uri file:///tmp/duckpipe_cluster_demo/duckpipe.db
"""

import os
from pathlib import Path

import duckdb
import polars as pl

from duckpipe import task

HERE = Path(__file__).parent
# See examples/01_daily_batch_etl/duck.py's comment on this override.
DATA = os.environ.get(
    "DUCKPIPE_EXAMPLE_DATA", str(HERE.parent / "data" / "nyc_taxi_sample.parquet")
)


@task(cache=True)
def extract():
    return duckdb.sql(f"SELECT * FROM read_parquet('{DATA}')").pl()


@task(cache=True)
def by_payment_type(trips=extract):
    return (
        trips.group_by("payment_type")
        .agg(trip_count=pl.len(), total_fare=pl.col("fare_amount").sum().round(2))
        .sort("payment_type")
    )


@task(cache=True)
def by_hour(trips=extract):
    return (
        trips.with_columns(pl.col("tpep_pickup_datetime").dt.hour().alias("hour"))
        .group_by("hour")
        .agg(trip_count=pl.len())
        .sort("hour")
    )


@task(cache=True)
def combined_report(payment=by_payment_type, hour=by_hour):
    return {"by_payment_type_rows": payment.height, "by_hour_rows": hour.height}


if __name__ == "__main__":
    from duckpipe import run

    run(__file__, db_path=HERE / "duckpipe.db")
