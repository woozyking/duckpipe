"""Nesting a pipeline inside a task -- and making that nesting visible,
to any depth.

``report_card``/``report_cash``'s bodies don't just crunch data
themselves: each calls `duckpipe.run()` on ``report_pipeline.py``, a
genuinely separate, independently-runnable pipeline -- once per payment
type, each with its own state file. ``report_pipeline.py`` doesn't stop
there either: its own last task nests a *third* pipeline,
``summary_pipeline.py`` (see that file's own docstring). Three real
levels, not a toy two -- nesting `duckpipe.run()` inside a task is safe
at any depth (DESIGN.md sec 11): a task's body always executes in a
thread-pool worker, never on the scheduler's own event loop, so an inner
run's own `asyncio.run()` never collides with an outer one, no matter
how many levels deep.

The payoff for actually nesting real pipelines here, instead of just
writing the same logic inline: every level stays independently runnable
and testable on its own
(``duckpipe run examples/09_nested_pipeline/report_pipeline.py``), with
its own real cache/state history -- and the outer pipeline's own
`duckpipe show --mermaid` can still show what's really inside every
nested call, all three levels, via `to_mermaid`'s recursive
`subgraphs=` parameter. See ``show_nested_mermaid.py`` in this folder
for exactly that, and README.md for the actual rendered diagram.

    uv run duckpipe run examples/09_nested_pipeline/pipeline.py --max-workers 1

``--max-workers 1`` matters here specifically, not as a general nesting
requirement: ``report_card``/``report_cash`` have no dependency edge, so
DuckPipe's default (unbounded concurrency for independent tasks) runs
them at once, and each communicates its payment type to its own nested
call through the *process-wide* ``DUCKPIPE_EXAMPLE_PAYMENT_TYPE``
environment variable (see ``report_pipeline.py``'s own docstring) --
exactly the "tasks that compete for the same resource" case DESIGN.md
sec 12 already documents needing this stated explicitly, not inferred.
Skip the flag and the two nested calls race on that shared variable,
caught directly (not assumed): running this without it reliably failed
with a state-file write-write conflict, from the two calls colliding on
each other's environment variable value mid-flight.
"""

import os
from pathlib import Path
from typing import Any

from duckpipe import run, task

HERE = Path(__file__).parent
REPORT_PIPELINE = HERE / "report_pipeline.py"


def _run_report(payment_type: int, db_name: str) -> dict[str, Any]:
    os.environ["DUCKPIPE_EXAMPLE_PAYMENT_TYPE"] = str(payment_type)
    summary = run(REPORT_PIPELINE, db_path=HERE / db_name)
    # `daily` comes from report_pipeline.py's own `aggregate`; `summary`
    # comes from *its* nested `summarize` task -- the third, innermost
    # nesting level (report_pipeline.py's own docstring), surfaced here
    # rather than left as a value nobody outside that pipeline ever uses.
    return {"daily": summary.results["aggregate"], "summary": summary.results["summarize"]}


@task(cache=True)
def report_card():
    return _run_report(1, "report_card.duckdb")  # 1 = credit card


@task(cache=True)
def report_cash():
    return _run_report(2, "report_cash.duckdb")  # 2 = cash


@task
def combine(card=report_card, cash=report_cash):
    return {"card": card, "cash": cash}


if __name__ == "__main__":
    # report_card/report_cash have no dependency edge, so DuckPipe's
    # default (unbounded concurrency for independent tasks) would run
    # both nested calls at once. report_pipeline.py's own explicit,
    # module-level `duckdb.connect()` (see its own docstring) means the
    # two nested runs never share a *connection* to race on -- but they
    # do still share this *process*, and each communicates its payment
    # type to its own nested call via the process-wide
    # `DUCKPIPE_EXAMPLE_PAYMENT_TYPE` environment variable. `max_workers=1`
    # is what keeps that hand-off from racing -- confirmed the hard way,
    # not assumed (see this module's own docstring): without it, the two
    # calls collide on each other's environment variable value mid-flight
    # and one nested run fails outright. DESIGN.md sec 12
    # ("Concurrency default") documents exactly this category of footgun
    # -- tasks that share a resource need this stated explicitly, since
    # DuckPipe has no way to infer "these shouldn't overlap" from the DAG
    # shape alone.
    run(__file__, db_path=HERE / "duckpipe.db", max_workers=1)
