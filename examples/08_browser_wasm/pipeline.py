"""Phase 4's first real deliverable (ROADMAP.md sec 8/11): a genuine
DuckPipe pipeline run entirely inside a browser tab via Pyodide -- no
server, no upload, whatever file you pick never leaves the tab. Runs
completely unmodified: `index.html` calls `duckpipe.run()` on this exact
file the same way `duckpipe run pipeline.py` would from a shell.

Schema-agnostic on purpose (unlike every other example's fixed NYC taxi
columns) -- the whole point of a browser demo is "drop in your own file"
-- so this profiles whatever tabular file it's given instead of
assuming particular columns.

    profile -> numeric_summary -> report
"""

import os

import duckdb

from duckpipe import task

# See examples/01_daily_batch_etl/duck.py's comment on this override --
# same convention, set here via pyodide.setEnviron() from index.html
# instead of a shell env var.
SOURCE = os.environ.get("DUCKPIPE_EXAMPLE_DATA", "/data/input.parquet")

_NUMERIC_TYPES = (
    "TINYINT", "SMALLINT", "INTEGER", "BIGINT", "HUGEINT", "FLOAT", "DOUBLE", "DECIMAL",
)


@task(cache=True)
def profile():
    """The one task that touches the whole file -- row count and column
    types, via `DESCRIBE`, not by reading the data into Python at all."""
    con = duckdb.connect()
    row_count = con.sql(f"SELECT count(*) FROM '{SOURCE}'").fetchone()[0]
    columns = con.sql(f"DESCRIBE SELECT * FROM '{SOURCE}'").fetchall()
    return {"row_count": row_count, "columns": [(name, dtype) for name, dtype, *_ in columns]}


@task(cache=True)
def numeric_summary(info=profile):
    """min/max/avg for every numeric column, computed by DuckDB itself in
    one query -- `profile` only supplies which columns qualify."""
    numeric_cols = [
        name for name, dtype in info["columns"] if dtype.split("(")[0] in _NUMERIC_TYPES
    ]
    if not numeric_cols:
        return {}
    aggs = ", ".join(
        f'min("{c}") AS "{c}_min", max("{c}") AS "{c}_max", avg("{c}") AS "{c}_avg"'
        for c in numeric_cols
    )
    row = duckdb.sql(f"SELECT {aggs} FROM '{SOURCE}'").fetchone()
    return {
        c: {"min": row[i * 3], "max": row[i * 3 + 1], "avg": round(row[i * 3 + 2], 3)}
        for i, c in enumerate(numeric_cols)
    }


@task(cache=True)
def report(info=profile, summary=numeric_summary):
    return {
        "rows": info["row_count"],
        "columns": len(info["columns"]),
        "numeric_columns_summarized": len(summary),
        "numeric_summary": summary,
    }


if __name__ == "__main__":
    from duckpipe import run

    run(__file__)
