"""The concrete demonstration of `to_mermaid`'s `subgraphs=` parameter.

`duckpipe show pipeline.py --mermaid` alone renders ``report_card``/
``report_cash`` as two plain, opaque nodes -- DuckPipe has no way to
know their bodies each run another pipeline (that would mean statically
analyzing arbitrary Python). This script states it explicitly instead,
using the exact sub-pipeline each task's own body calls, and renders
what's really inside each.

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

    outer_status = _last_status(HERE / "duckpipe.db", [t.name for t in outer_order])
    subgraphs = {
        "report_card": (sub_order, _last_status(HERE / "report_card.duckdb", sub_names)),
        "report_cash": (sub_order, _last_status(HERE / "report_cash.duckdb", sub_names)),
    }

    print(to_mermaid(outer_order, last_status=outer_status, subgraphs=subgraphs))


if __name__ == "__main__":
    main()
