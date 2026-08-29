"""A small, genuinely standalone pipeline: daily trip count and revenue,
optionally filtered to one payment type. Runnable completely on its own
-- with its own explicit ``--db``, the same convention every other
example uses (e.g. examples/01_daily_batch_etl/duck.py), and for the
same reason: the CLI's own default ("duckpipe.db" next to the pipeline
file) would otherwise collide with pipeline.py's own default state file
in this same directory:

    uv run duckpipe run examples/09_nested_pipeline/report_pipeline.py \\
        --db examples/09_nested_pipeline/report.duckdb

...or nested inside another pipeline's own task (see ``pipeline.py`` in
this same folder) -- nesting `duckpipe.run()` inside a task is safe
(DESIGN.md sec 11), and this file never needs to know which is
happening; it's a normal pipeline either way.

``DUCKPIPE_EXAMPLE_PAYMENT_TYPE`` mirrors ``DUCKPIPE_EXAMPLE_DATA``'s own
env-var-override convention (examples/01_daily_batch_etl/duck.py):
`duckpipe.run()` never passes ad hoc arguments into a root task, so an
external parameter goes through an environment variable instead.

Uses its own explicit ``duckdb.connect()`` rather than the bare
``duckdb.sql()`` module-level default connection examples/01 uses --
that default is one connection shared by the *whole process*, and a
standalone script reloads fresh (a new module object, re-executed top
to bottom) on every `duckpipe.run()` call, including a nested one
(DESIGN.md sec 5's `to_mermaid`/`subgraphs` note): two nested runs of
this same file, even run strictly sequentially, aren't guaranteed to
execute in the same worker thread, and DuckDB's own connection isn't
safe to hand between threads that way. A fresh, module-level connection
made *inside* this reload sidesteps that -- confirmed the hard way, not
assumed (see `pipeline.py`'s own `max_workers=1` note for the
concurrency half of this).
"""

import os
from pathlib import Path

import duckdb

from duckpipe import run, task

HERE = Path(__file__).parent
DATA = os.environ.get(
    "DUCKPIPE_EXAMPLE_DATA", str(HERE.parent / "data" / "nyc_taxi_sample.parquet")
)
PAYMENT_TYPE = os.environ.get("DUCKPIPE_EXAMPLE_PAYMENT_TYPE")  # unset = every payment type
_con = duckdb.connect()


@task
def extract():
    return _con.sql(f"SELECT * FROM read_parquet('{DATA}')")


@task
def clean(trips=extract):
    rel = trips.filter("fare_amount > 0 AND trip_distance > 0")
    if PAYMENT_TYPE is not None:
        rel = rel.filter(f"payment_type = {int(PAYMENT_TYPE)}")
    return rel.select("tpep_pickup_datetime::DATE AS trip_date, fare_amount")


@task(cache=True)
def aggregate(daily=clean):
    rel = daily.aggregate(
        "trip_date, count(*) AS trip_count, round(sum(fare_amount), 2) AS total_revenue"
    ).order("trip_date")
    # A plain list of dicts, not a live DuckDBPyRelation -- this is the
    # value a *nesting* task gets back from `summary.results["aggregate"]`
    # once this pipeline's own `run()` returns, so it needs to be a
    # normal, picklable value, the same as any other task's output.
    return [
        {"trip_date": str(trip_date), "trip_count": trip_count, "total_revenue": total_revenue}
        for trip_date, trip_count, total_revenue in rel.fetchall()
    ]


if __name__ == "__main__":
    run(__file__, db_path=HERE / "report.duckdb")
