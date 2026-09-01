"""The concrete demonstration of `to_mermaid`'s recursive `subgraphs=`
parameter -- three real levels deep, not just one.

`duckpipe show pipeline.py --mermaid` alone renders ``report_card``/
``report_cash`` as two plain, opaque nodes -- DuckPipe has no way to
know their bodies each run another pipeline (that would mean statically
analyzing arbitrary Python), let alone that *that* pipeline's own last
task nests a third one. This script states all of it explicitly
instead, using the exact sub-pipeline each level's own body calls, and
renders what's really inside, all the way down.

    uv run python examples/09_nested_pipeline/show_nested_mermaid.py

If ``pipeline.py`` has already been run at least once, the nested nodes
are colored by that sub-pipeline's own real run history (its own state
file, not the outer one) -- run it first to see that; otherwise this
still prints a plain, uncolored diagram of the DAG's shape alone. Paste
the output into any Markdown that renders Mermaid (GitHub, GitLab, ...),
or see README.md in this folder for the already-rendered result.
"""

from pathlib import Path

from duckpipe import build_dag, to_mermaid
from duckpipe.state import StateStore

HERE = Path(__file__).parent


def _last_status(db_path: Path, task_names: list[str]) -> dict[str, tuple[str, str]] | None:
    if not db_path.exists():
        return None
    with StateStore(db_path, read_only=True) as store:
        statuses = {}
        for name in task_names:
            row = store.last_status(name)
            if row:
                statuses[name] = row
        return statuses


def main() -> None:
    outer_order = build_dag(HERE / "pipeline.py").topological_order()
    sub_order = build_dag(HERE / "report_pipeline.py").topological_order()
    sub_names = [t.name for t in sub_order]
    innermost_order = build_dag(HERE / "summary_pipeline.py").topological_order()
    innermost_names = [t.name for t in innermost_order]

    outer_status = _last_status(HERE / "duckpipe.db", [t.name for t in outer_order])

    def report_subgraph(db_name: str, summary_db_name: str) -> tuple:
        # The third, optional tuple element is `report_pipeline.py`'s own
        # `subgraphs` dict -- exactly the same shape as the outer one,
        # just describing its own `summarize` task instead of
        # `report_card`/`report_cash`. Recursion isn't a separate case.
        innermost_status = _last_status(HERE / summary_db_name, innermost_names)
        return (
            sub_order,
            _last_status(HERE / db_name, sub_names),
            {"summarize": (innermost_order, innermost_status)},
        )

    subgraphs = {
        "report_card": report_subgraph("report_card.duckdb", "summary_1.duckdb"),
        "report_cash": report_subgraph("report_cash.duckdb", "summary_2.duckdb"),
    }

    print(to_mermaid(outer_order, last_status=outer_status, subgraphs=subgraphs))


if __name__ == "__main__":
    main()
