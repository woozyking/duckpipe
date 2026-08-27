"""What the DuckLake backend (DESIGN.md sec 8) actually buys
over a plain `.duckdb` file: run `pipeline.py` at least once (twice, to
see a "skipped" entry too) before running this.

    uv run duckpipe run examples/06_ducklake_observability/pipeline.py \\
        --db "ducklake:sqlite:examples/06_ducklake_observability/pipeline.ducklake.sqlite"
    uv run duckpipe run examples/06_ducklake_observability/pipeline.py \\
        --db "ducklake:sqlite:examples/06_ducklake_observability/pipeline.ducklake.sqlite"
    uv run python examples/06_ducklake_observability/explore_history.py
"""

from pathlib import Path

from duckpipe.state import StateStore

HERE = Path(__file__).parent
CATALOG = f"ducklake:sqlite:{HERE / 'pipeline.ducklake.sqlite'}"


def main() -> None:
    with StateStore(CATALOG, read_only=True) as store:
        print("=== snapshot history -- every run, every task, one readable line each ===")
        snapshots = store.snapshots()
        for snap in snapshots:
            if snap["commit_message"]:
                print(f"  [{snap['snapshot_id']}] {snap['commit_message']}")

        print("\n=== time travel: task_runs right after `extract` first succeeded ===")
        first_extract = next(
            s["snapshot_id"] for s in snapshots if s["commit_message"] == "task extract succeeded"
        )
        rows = store.con.execute(
            f"SELECT task_name, status FROM task_runs AT (VERSION => {first_extract})"
        ).fetchall()
        print(f"  (as of snapshot {first_extract}, before anything downstream had run):", rows)
        print("  (the current, full task_runs has all of that plus everything since)")

    print("\n=== schema evolution: no migration step ===")
    # A plain ALTER TABLE, same as you'd run against any DuckDB database --
    # DuckPipe's own schema doesn't need or expect this column, so adding
    # it doesn't collide with anything duckpipe itself writes.
    with StateStore(CATALOG) as store:
        store.con.execute("ALTER TABLE task_runs ADD COLUMN IF NOT EXISTS host VARCHAR")
        print("  added `host` column to task_runs -- existing rows get NULL, nothing rewritten")
        print(" ", store.con.execute("DESCRIBE task_runs").fetchall())


if __name__ == "__main__":
    main()
