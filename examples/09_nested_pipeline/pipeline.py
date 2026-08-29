"""Nesting a pipeline inside a task -- and making that nesting visible.

``report_card``/``report_cash``'s bodies don't just crunch data
themselves: each calls `duckpipe.run()` on ``report_pipeline.py``, a
genuinely separate, independently-runnable pipeline -- once per payment
type, each with its own state file. Nesting `duckpipe.run()` inside a
task is safe (DESIGN.md sec 11): a task's body always executes in a
thread-pool worker, never on the scheduler's own event loop, so the
inner run's own `asyncio.run()` never collides with the outer one.

The payoff for actually nesting a real pipeline here, instead of just
writing the same logic inline: ``report_pipeline.py`` stays
independently runnable and testable
(``duckpipe run examples/09_nested_pipeline/report_pipeline.py``), with
its own real cache/state history -- and the outer pipeline's own
`duckpipe show --mermaid` can still show what's really inside each
nested call, via `to_mermaid`'s `subgraphs=` parameter. See
``show_nested_mermaid.py`` in this folder for exactly that, and
README.md for the actual rendered diagram.

    uv run duckpipe run examples/09_nested_pipeline/pipeline.py
"""

import os
from pathlib import Path
from typing import Any

from duckpipe import run, task

HERE = Path(__file__).parent
REPORT_PIPELINE = HERE / "report_pipeline.py"


def _run_report(payment_type: int, db_name: str) -> list[dict[str, Any]]:
    os.environ["DUCKPIPE_EXAMPLE_PAYMENT_TYPE"] = str(payment_type)
    summary = run(REPORT_PIPELINE, db_path=HERE / db_name)
    return summary.results["aggregate"]


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
    # default (unbounded concurrency for independent tasks) runs both
    # nested calls at once -- confirmed safe here (5/5 clean runs),
    # since report_pipeline.py's own explicit, module-level
    # `duckdb.connect()` (see its own docstring) is fresh on every
    # reload, including a nested one, so the two nested runs never
    # share a connection to race on in the first place. `max_workers=1`
    # is kept anyway as a cheap belt-and-suspenders default for nesting
    # in general (DESIGN.md sec 12, "Concurrency default") -- it's not
    # what fixes the bug this example actually hit; the connection fix
    # is.
    run(__file__, db_path=HERE / "duckpipe.db", max_workers=1)
