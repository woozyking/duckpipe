"""The exact shape of examples/01_daily_batch_etl/duck.py, pointed at a
DuckLake catalog instead of a plain file -- DuckPipe's opt-in
observability upgrade (DESIGN.md sec 8). Nothing about the task code
changes; only `db_path` does.

Run it, then run `explore_history.py` in this folder to see what that
buys you: time travel over every run, no migration step for the schema
change it makes partway through.

    uv run duckpipe run examples/06_ducklake_observability/pipeline.py \\
        --db "ducklake:sqlite:examples/06_ducklake_observability/pipeline.ducklake.sqlite"
    uv run python examples/06_ducklake_observability/explore_history.py
"""

import os
from pathlib import Path

import duckdb

from duckpipe import task

HERE = Path(__file__).parent
DATA = os.environ.get(
    "DUCKPIPE_EXAMPLE_DATA", str(HERE.parent / "data" / "nyc_taxi_sample.parquet")
)


@task
def extract():
    return duckdb.sql(f"SELECT * FROM read_parquet('{DATA}')")


@task
def clean(trips=extract):
    return trips.filter("fare_amount > 0 AND trip_distance > 0")


@task(cache=True)
def daily_totals(daily=clean):
    return daily.aggregate(
        "tpep_pickup_datetime::DATE AS trip_date, "
        "count(*) AS trip_count, round(sum(fare_amount), 2) AS total_fare"
    ).pl()


if __name__ == "__main__":
    from duckpipe import run

    run(
        __file__,
        db_path=f"ducklake:sqlite:{HERE / 'pipeline.ducklake.sqlite'}",
    )
