"""The innermost of the three real nesting levels this example
demonstrates: ``pipeline.py`` nests ``report_pipeline.py``, which nests
this file. A small, standalone pipeline that rolls a daily aggregate
down to one summary record -- total trips, total revenue, and the
single best day by revenue.

Runnable completely on its own, same convention as
``report_pipeline.py``:

    uv run duckpipe run examples/09_nested_pipeline/summary_pipeline.py \\
        --db examples/09_nested_pipeline/summary.duckdb

...or nested inside ``report_pipeline.py``'s own ``summarize`` task --
this file never needs to know which is happening; it's a normal
pipeline either way.

``DUCKPIPE_EXAMPLE_DAILY_JSON`` mirrors ``report_pipeline.py``'s own
``DUCKPIPE_EXAMPLE_PAYMENT_TYPE`` convention: `duckpipe.run()` never
passes ad hoc arguments into a root task, so the daily rows a nesting
caller already computed cross in as a JSON string instead of asking
this pipeline to go compute (or re-fetch) them itself. Unset, this
pipeline still runs standalone against a small built-in default -- the
same "genuinely runnable on its own" bar every other example holds to.
"""

import json
import os
from pathlib import Path

from duckpipe import run, task

HERE = Path(__file__).parent

_STANDALONE_DEFAULT = [
    {"trip_date": "2024-01-01", "trip_count": 100, "total_revenue": 1234.56},
    {"trip_date": "2024-01-02", "trip_count": 80, "total_revenue": 987.65},
]
DAILY_JSON = os.environ.get("DUCKPIPE_EXAMPLE_DAILY_JSON")


@task
def load_daily() -> list[dict]:
    return json.loads(DAILY_JSON) if DAILY_JSON is not None else _STANDALONE_DEFAULT


@task(cache=True)
def totals(daily=load_daily) -> dict:
    best_day = max(daily, key=lambda row: row["total_revenue"])
    return {
        "total_trips": sum(row["trip_count"] for row in daily),
        "total_revenue": round(sum(row["total_revenue"] for row in daily), 2),
        "best_day": best_day["trip_date"],
    }


if __name__ == "__main__":
    run(__file__, db_path=HERE / "summary.duckdb")
