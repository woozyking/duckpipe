"""Daily batch ETL over NYC TLC yellow taxi trip data.

extract -> clean -> load, the shape of most "batch ETL" pipelines. `load`
writes into its own small warehouse file, separate from DuckPipe's own
`duckpipe.db` state -- the pipeline's data and the orchestrator's
metadata never share a file.

`extract`/`clean` return a lazy `DuckDBPyRelation` and skip `cache=True`
on purpose: a lazy relation holds a live query-plan handle, not a plain
value, so it isn't picklable, and rebuilding the plan itself is nearly
free anyway (no rows move until something materializes it). `load` is
where real work happens (materializing the aggregate into the warehouse
table), so that's the one task worth fingerprinting a cached result for.

Run it:

    uv run duckpipe run examples/01_daily_batch_etl/pipeline.py

Run it again immediately and `load` reports "skipped" -- nothing about
the source data or task code changed, so the fingerprint-based
incrementality (ROADMAP.md tenet #6) has nothing to do.
"""

import os
from pathlib import Path

import duckdb

from duckpipe import task

HERE = Path(__file__).parent
# Same code path locally and at scale (ROADMAP.md tenet #5): point this at
# the full public dataset (e.g. the CloudFront URL in ../data/README.md)
# to run against 49M rows instead of the bundled ~12k-row sample -- DuckDB
# streams a remote parquet URL directly, no download or code change needed.
DATA = os.environ.get(
    "DUCKPIPE_EXAMPLE_DATA", str(HERE.parent / "data" / "nyc_taxi_sample.parquet")
)
WAREHOUSE = HERE / "warehouse.duckdb"


@task
def extract():
    return duckdb.sql(f"SELECT * FROM read_parquet('{DATA}')")


@task
def clean(trips=extract):
    return trips.filter(
        "fare_amount > 0 AND trip_distance > 0 AND passenger_count > 0"
    ).select(
        "tpep_pickup_datetime::DATE AS trip_date, "
        "payment_type, fare_amount, tip_amount, total_amount"
    )


@task(cache=True)
def load(daily=clean):
    # `daily` is a relation on the implicit default connection (the same
    # one `duckdb.sql()` used in extract/clean) -- ATTACH the warehouse
    # file onto that *same* connection rather than opening a second one;
    # a DuckDBPyRelation can't be referenced from a different connection
    # than the one that created it.
    duckdb.sql(f"ATTACH '{WAREHOUSE}' AS warehouse")
    duckdb.sql(
        """
        CREATE OR REPLACE TABLE warehouse.daily_fares AS
        SELECT
            trip_date,
            count(*) AS trip_count,
            round(sum(fare_amount), 2) AS total_fare,
            round(sum(tip_amount), 2) AS total_tip,
            round(avg(total_amount), 2) AS avg_total
        FROM daily
        GROUP BY trip_date
        ORDER BY trip_date
        """
    )
    row_count = duckdb.sql("SELECT count(*) FROM warehouse.daily_fares").fetchone()[0]
    duckdb.sql("DETACH warehouse")
    return row_count


if __name__ == "__main__":
    from duckpipe import run

    run(__file__)
