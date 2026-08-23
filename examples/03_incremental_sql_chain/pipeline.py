"""An incremental SQL transform chain, in the SQLMesh sense that inspired
DuckPipe's fingerprinting (ROADMAP.md sec 13): every stage is eager and
`cache=True`, so re-running with no code or data changes skips the *whole*
chain -- contrast with example 01, where only the final materialization
step was worth caching because the earlier steps stayed lazy.

    extract_payment_types -> daily_revenue -> rolling_revenue -> report

Each stage returns a plain, picklable `polars.DataFrame` rather than a
live DuckDB relation -- that's what makes `cache=True` meaningful here:
skipping a stage means literally reusing last run's DataFrame from
`duckpipe.db`, no DuckDB connection involved at all on a cache hit.

Run it twice -- the second run reports every stage "skipped":

    uv run duckpipe run examples/03_incremental_sql_chain/pipeline.py
    uv run duckpipe run examples/03_incremental_sql_chain/pipeline.py
    uv run duckpipe run examples/03_incremental_sql_chain/pipeline.py --force  # re-run anyway
"""

import os
from pathlib import Path

import duckdb
import polars as pl

from duckpipe import task

HERE = Path(__file__).parent
# See examples/01_daily_batch_etl/pipeline.py's comment on this override.
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
    rel = duckdb.sql(
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
    return rel.pl()


@task(cache=True)
def rolling_revenue(daily=daily_revenue):
    return (
        daily.sort("trip_date")
        .with_columns(
            pl.col("revenue")
            .rolling_mean(window_size=3, min_samples=1)
            .over("payment_type")
            .alias("revenue_3d_avg")
        )
        .with_columns(
            pl.col("payment_type").replace_strict(PAYMENT_TYPE_LABELS, default="Other").alias(
                "payment_type_label"
            )
        )
    )


@task(cache=True)
def report(rolling=rolling_revenue):
    rolling.write_csv(REPORT)
    return {"rows": rolling.height, "path": str(REPORT)}


if __name__ == "__main__":
    from duckpipe import run

    run(__file__)
