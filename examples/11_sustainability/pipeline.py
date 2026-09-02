"""A genuinely demanding real-world job -- not to showcase a facet of
DuckPipe's own API (every other example does that), but to be the fixed
point in a sustainability comparison: the *same* job, the *same*
plain-function invocation shape (`run_pipeline.py`), run once as a
plain DuckPipe pipeline with nothing standing behind it, versus the
identical invocation sitting behind a conventional always-on
orchestrator. See ``measure_and_quantify.py`` and this folder's own
README for the actual comparison and the real, cited numbers behind it.

    extract -> clean -> join_boroughs -> aggregate -> report

Borough-level daily trip counts and revenue, joined against the real
zone lookup table, for however many months of the actual public NYC TLC
dataset ``DUCKPIPE_EXAMPLE_DATA`` names (comma-separated URLs or paths;
see ``examples/data/README.md`` for the full-dataset URL pattern this
extends). Falls back to the bundled ~12k-row sample -- fine for a
correctness check, not the point: the real, demanding run points this
at a full year of real trip data (~35M rows), a comma-separated list of
twelve real monthly URLs, straight off NYC TLC's own CDN. DuckDB streams
each one; nothing is downloaded to disk first.

Stays lazy through `join_boroughs` -- only `aggregate` (the one step
that actually needs a materialized answer) is worth `cache=True`,
matching every other example's own convention.
"""

import os
from pathlib import Path

import duckdb

from duckpipe import task

HERE = Path(__file__).parent
LOOKUP = HERE.parent / "data" / "taxi_zone_lookup.csv"
_DEFAULT = str(HERE.parent / "data" / "nyc_taxi_sample.parquet")
SOURCES = os.environ.get("DUCKPIPE_EXAMPLE_DATA", _DEFAULT).split(",")
_SOURCE_LIST = ", ".join(f"'{s}'" for s in SOURCES)


@task
def extract():
    """The one task that touches the network (or disk, for the bundled
    sample) -- every other task operates on this relation's own lazy
    query plan, nothing materializes here."""
    return duckdb.sql(
        "SELECT tpep_pickup_datetime, PULocationID, fare_amount, trip_distance "
        f"FROM read_parquet([{_SOURCE_LIST}], union_by_name=true)"
    )


@task
def clean(trips=extract):
    return trips.filter("fare_amount > 0 AND trip_distance > 0 AND PULocationID IS NOT NULL")


@task
def join_boroughs(trips=clean):
    """The genuinely demanding step: a real join against the zone
    lookup table, not a canned aggregate DuckDB could push straight to
    a single scan -- the same shape datapunk's own suite_03 (a real
    fact/dimension join) uses, for the same reason: it's what makes
    this a believable stand-in for "the kind of job a team reaches for
    a cluster over," not a toy."""
    zones = duckdb.sql(f"SELECT LocationID, Borough FROM read_csv('{LOOKUP}')")
    return trips.join(zones, "PULocationID = LocationID").select(
        "Borough, tpep_pickup_datetime, fare_amount"
    )


@task(cache=True)
def aggregate(trips=join_boroughs):
    """The one task that actually materializes an answer -- everything
    upstream is still just a query plan until this `.fetchall()`."""
    rel = trips.aggregate(
        "Borough, date_trunc('day', tpep_pickup_datetime) AS day, "
        "count(*) AS trips, round(sum(fare_amount), 2) AS revenue",
        "Borough, day",
    )
    return [
        {"borough": b, "day": str(d), "trips": t, "revenue": r} for b, d, t, r in rel.fetchall()
    ]


@task(cache=True)
def report(daily=aggregate):
    total_trips = sum(row["trips"] for row in daily)
    total_revenue = round(sum(row["revenue"] for row in daily), 2)
    by_borough: dict[str, float] = {}
    for row in daily:
        by_borough[row["borough"]] = round(by_borough.get(row["borough"], 0) + row["revenue"], 2)
    top_borough = max(by_borough, key=by_borough.get) if by_borough else None
    return {
        "sources": len(SOURCES),
        "day_borough_rows": len(daily),
        "total_trips": total_trips,
        "total_revenue": total_revenue,
        "top_borough_by_revenue": top_borough,
    }


if __name__ == "__main__":
    from duckpipe import run

    run(__file__, db_path=HERE / "duckpipe.db")
