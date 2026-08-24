"""Daily batch ETL over NYC TLC yellow taxi trip data -- Polars engine.

Apples-to-apples with duck.py in this same folder: same source data, same
`extract -> clean -> load` shape, same output shape. Compare how the two
engines express it -- notably, there's no DuckDB-style "which connection
created this relation" concern here, since a Polars `LazyFrame` isn't
tied to a live handle at all.

extract/clean/load stay lazy the whole way through: `extract`/`clean`
return a `LazyFrame` (a query plan, not rows), and `load`'s
`.sink_parquet()` streams the aggregate straight to disk without ever
materializing the full result in memory -- streaming all the way, not
just "lazy until collect()". `load` is the one task worth `cache=True`:
same reasoning as duck.py, a lazy plan is nearly free to rebuild, so
there's nothing to gain caching `extract`/`clean` themselves.

Run it (from the repo root):

    uv run duckpipe run examples/01_daily_batch_etl/pl.py \
        --db examples/01_daily_batch_etl/duckpipe.pl.db

Run it again immediately and `load` reports "skipped".
"""

import os
from pathlib import Path

import polars as pl

from duckpipe import task

HERE = Path(__file__).parent
# Same override as duck.py -- see its docstring and ../data/README.md.
DATA = os.environ.get(
    "DUCKPIPE_EXAMPLE_DATA", str(HERE.parent / "data" / "nyc_taxi_sample.parquet")
)
WAREHOUSE = HERE / "warehouse.pl.parquet"


@task
def extract():
    return pl.scan_parquet(DATA)


@task
def clean(trips=extract):
    return trips.filter(
        (pl.col("fare_amount") > 0)
        & (pl.col("trip_distance") > 0)
        & (pl.col("passenger_count") > 0)
    ).select(
        pl.col("tpep_pickup_datetime").dt.date().alias("trip_date"),
        "payment_type",
        "fare_amount",
        "tip_amount",
        "total_amount",
    )


@task(cache=True)
def load(daily=clean):
    daily.group_by("trip_date").agg(
        trip_count=pl.len(),
        total_fare=pl.col("fare_amount").sum().round(2),
        total_tip=pl.col("tip_amount").sum().round(2),
        avg_total=pl.col("total_amount").mean().round(2),
    ).sort("trip_date").sink_parquet(WAREHOUSE)
    return pl.scan_parquet(WAREHOUSE).select(pl.len()).collect().item()


if __name__ == "__main__":
    from duckpipe import run

    run(__file__, db_path=HERE / "duckpipe.pl.db")
