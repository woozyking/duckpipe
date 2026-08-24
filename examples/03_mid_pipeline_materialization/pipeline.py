"""Mid-pipeline materialization: when it's actually worth breaking laziness.

    daily_revenue (DuckDB, lazy -> MATERIALIZE) -> rolling_revenue (Polars) -> report (Polars)

Examples 01 and 02 stay lazy/streaming end to end because there's
nothing to gain from an eager checkpoint anywhere in them. This example
is the deliberate exception, and the reasoning for exactly where is the
point of it:

`daily_revenue` is the only task here that touches the full,
potentially-millions-of-rows source file -- everything after it works
off a tiny per-day/per-payment-type summary (a handful of rows). That's
the one place a re-run would otherwise redo real, non-trivial work, so
it's the one place `cache=True` earns its keep: skip-if-unchanged needs
an eager, picklable value to skip *to* (a live query plan can't be
cached), and this is where materializing one actually pays for the
pickling/unpickling cost. `rolling_revenue` and `report` stay plain,
uncached tasks -- they're cheap enough to redo every run regardless of
whether `daily_revenue` was skipped, so caching them too would just
spend disk and pickle time for no real benefit.

This also happens to be an entirely ordinary demonstration of the core
being data-blind (ROADMAP.md sec 2, tenet #2): `daily_revenue` hands off
from DuckDB to Polars with zero glue code -- `.pl()` moves the result
across the Arrow boundary once, and nothing about the DAG or the
scheduler cares that two different engines were involved.

Run it twice -- the second run reports `daily_revenue` as "skipped";
`rolling_revenue`/`report` re-run (they're uncached, by design) but
produce byte-for-byte identical output since their input didn't change:

    uv run duckpipe run examples/03_mid_pipeline_materialization/pipeline.py
    uv run duckpipe run examples/03_mid_pipeline_materialization/pipeline.py
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
REPORT = HERE / "report.csv"

PAYMENT_TYPE_LABELS = {
    1: "Credit card",
    2: "Cash",
    3: "No charge",
    4: "Dispute",
    5: "Unknown",
    6: "Voided trip",
}


@task(cache=True)
def daily_revenue():
    # The one full-file scan+aggregation in this pipeline -- lazy right up
    # until `.pl()`, which is the deliberate materialization point. A
    # plain `polars.DataFrame` pickles (and round-trips its type) as-is;
    # `cache_backend="arrow"` is available too, but a cache hit under it
    # always hands back a plain `pyarrow.Table` (ROADMAP.md sec 6.2)
    # instead of the original DataFrame, so `rolling_revenue` below would
    # need to `pl.from_arrow(...)` it first -- not worth the extra step
    # for a table this small.
    lazy = duckdb.sql(
        f"""
        SELECT
            tpep_pickup_datetime::DATE AS trip_date,
            payment_type,
            sum(total_amount) AS revenue,
            count(*) AS trip_count
        FROM read_parquet('{DATA}')
        WHERE fare_amount > 0
        GROUP BY 1, 2
        ORDER BY 1, 2
        """
    )
    return lazy.pl()  # DuckDB -> Polars handoff over Arrow; no glue code


@task
def rolling_revenue(daily=daily_revenue):
    return daily.sort("trip_date").with_columns(
        pl.col("revenue")
        .rolling_mean(window_size=3, min_samples=1)
        .over("payment_type")
        .alias("revenue_3d_avg"),
        pl.col("payment_type").replace_strict(PAYMENT_TYPE_LABELS, default="Other").alias(
            "payment_type_label"
        ),
    )


@task
def report(rolling=rolling_revenue):
    rolling.write_csv(REPORT)
    return {"rows": rolling.height, "path": str(REPORT)}


if __name__ == "__main__":
    from duckpipe import run

    run(__file__, db_path=HERE / "duckpipe.db")
