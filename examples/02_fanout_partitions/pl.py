"""Fan-out partition processing: one task per NYC borough -- Polars engine.

Apples-to-apples with duck.py in this same folder: same source data, same
per-borough fan-out shape, same output shape. The interesting contrast:
there's no DuckDB-style "shared connection, take your own cursor()" story
here at all -- every `pl.scan_parquet(...)` call below builds an
independent lazy plan with no connection object to share or race on, so
fan-out safety is simply a non-issue for this engine. Polars' own engine
still releases the GIL during execution, so these partition tasks get
the same genuine concurrency duck.py's cursor-per-task pattern gets.

Every partition task's `.sink_parquet()` streams its filtered slice
straight to disk -- lazy, then streaming, all the way through; nothing
about a single partition's data is ever pulled fully into memory.

Run it (from the repo root):

    uv run duckpipe run examples/02_fanout_partitions/pl.py \
        --db examples/02_fanout_partitions/duckpipe.pl.db
"""

import os
from pathlib import Path

import polars as pl

from duckpipe import task

HERE = Path(__file__).parent
# See examples/01_daily_batch_etl/pl.py's comment on this override.
DATA = os.environ.get(
    "DUCKPIPE_EXAMPLE_DATA", str(HERE.parent / "data" / "nyc_taxi_sample.parquet")
)
ZONES = HERE.parent / "data" / "taxi_zone_lookup.csv"
OUTPUT = HERE / "output" / "pl"


def _slug(name: str) -> str:
    return name.lower().replace(" ", "_")


def _with_borough(lf: pl.LazyFrame) -> pl.LazyFrame:
    return lf.join(pl.scan_csv(ZONES), left_on="PULocationID", right_on="LocationID")


_boroughs = (
    _with_borough(pl.scan_parquet(DATA))
    .filter(~pl.col("Borough").is_in(["Unknown", "N/A"]))
    .select("Borough")
    .unique()
    .sort("Borough")
    .collect()["Borough"]
    .to_list()
)

partition_tasks = []
for _borough in _boroughs:

    @task(name=f"process_{_slug(_borough)}", cache=True)
    def process_partition(borough=_borough):
        OUTPUT.mkdir(parents=True, exist_ok=True)
        out_path = OUTPUT / f"{_slug(borough)}.parquet"

        _with_borough(pl.scan_parquet(DATA)).filter(pl.col("Borough") == borough).sink_parquet(
            out_path
        )
        row_count = pl.scan_parquet(out_path).select(pl.len()).collect().item()
        return {"borough": borough, "rows": row_count, "path": str(out_path)}

    partition_tasks.append(process_partition)


@task(depends_on=partition_tasks)
def combine():
    return {"partitions": len(partition_tasks), "boroughs": _boroughs}


if __name__ == "__main__":
    from duckpipe import run

    run(__file__, db_path=HERE / "duckpipe.pl.db")
