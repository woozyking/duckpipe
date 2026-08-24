"""Fan-out partition processing: one task per NYC borough -- DuckDB engine.

Apples-to-apples with pl.py in this same folder: same source data, same
per-borough fan-out shape, same output shape.

    peek boroughs -> {process_<borough> for each borough} -> combine

Figuring out *how many* partitions to create is an ordinary Python
question that has to be answered before the DAG can even be built, so it
happens as a plain query at module-import time, not inside a task -- this
is inherent to any dynamic-fan-out design (Airflow's dynamic task mapping
and Prefect's `.map()` have the same shape), not a DuckPipe limitation.
This is the pattern ROADMAP.md sec 4 recommends for large fan-out
instead of a dynamic-mapping primitive baked into the core.

Every per-borough task takes its own `cursor()` off one shared DuckDB
connection -- the documented-safe way for independent tasks to use
DuckDB concurrently (ROADMAP.md sec 7) -- so the partitions can run in
parallel without racing on a single connection object. Each task's own
query plan stays lazy right up until `.write_parquet()` executes it.

Run it (from the repo root):

    uv run duckpipe run examples/02_fanout_partitions/duck.py \
        --db examples/02_fanout_partitions/duckpipe.duck.db
"""

import os
from pathlib import Path

import duckdb

from duckpipe import task

HERE = Path(__file__).parent
# See examples/01_daily_batch_etl/duck.py's comment on this override --
# same idea, so this fan-out can be re-run over the full public dataset.
DATA = os.environ.get(
    "DUCKPIPE_EXAMPLE_DATA", str(HERE.parent / "data" / "nyc_taxi_sample.parquet")
)
ZONES = HERE.parent / "data" / "taxi_zone_lookup.csv"
OUTPUT = HERE / "output" / "duck"

_con = duckdb.connect()


def _slug(name: str) -> str:
    return name.lower().replace(" ", "_")


_boroughs = [
    row[0]
    for row in _con.execute(
        f"""
        SELECT DISTINCT z.Borough
        FROM read_parquet('{DATA}') t
        JOIN read_csv('{ZONES}') z ON t.PULocationID = z.LocationID
        WHERE z.Borough NOT IN ('Unknown', 'N/A')
        ORDER BY 1
        """
    ).fetchall()
]

partition_tasks = []
for _borough in _boroughs:

    @task(name=f"process_{_slug(_borough)}", cache=True)
    def process_partition(borough=_borough):
        OUTPUT.mkdir(parents=True, exist_ok=True)
        out_path = OUTPUT / f"{_slug(borough)}.parquet"

        cur = _con.cursor()
        rel = cur.sql(
            f"""
            SELECT t.*
            FROM read_parquet('{DATA}') t
            JOIN read_csv('{ZONES}') z ON t.PULocationID = z.LocationID
            WHERE z.Borough = $borough
            """,
            params={"borough": borough},
        )
        rel.write_parquet(str(out_path))
        row_count = rel.aggregate("count(*)").fetchone()[0]
        cur.close()
        return {"borough": borough, "rows": row_count, "path": str(out_path)}

    partition_tasks.append(process_partition)


@task(depends_on=partition_tasks)
def combine():
    return {"partitions": len(partition_tasks), "boroughs": _boroughs}


if __name__ == "__main__":
    from duckpipe import run

    run(__file__, db_path=HERE / "duckpipe.duck.db")
