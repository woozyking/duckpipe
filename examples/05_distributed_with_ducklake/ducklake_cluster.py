"""What a DuckLake-backed state store (ROADMAP.md sec 8, Phase 3b) would
give you over Phase 3a's delta-merge mechanism (see
../04_distributed_cluster): every worker commits straight into one
shared, ACID-tracked table -- no `.pending/` directory, no absorb step,
no `duckpipe compact`. This is a standalone demonstration of that
coordination mechanism, not a literal `duckpipe.run()` backend -- wiring
DuckLake into DuckPipe's own state store is real, separate engineering
(ROADMAP.md sec 11, Phase 3b), not something this example claims to do.

The honest part, verified directly before writing this example (not
assumed): a SQLite catalog throws "database is locked" often enough under
real concurrent commits that `worker.py` needs a retry loop -- 8
concurrent writers against a fresh SQLite-catalog DuckLake failed 3 of 8
attempts on the first try, no retry. A Postgres catalog gives lock-free
concurrent commits at real scale; that's the natural next step if
retrying isn't enough for your write volume, and needs a reachable
Postgres instance this repo doesn't try to provide.

Run: `uv run python examples/05_distributed_with_ducklake/ducklake_cluster.py`
"""

import shutil
import subprocess
import sys
from pathlib import Path

import duckdb

HERE = Path(__file__).parent
CATALOG = HERE / "catalog.sqlite"
DATA_PATH = HERE / "data"
TASKS = ["extract", "by_payment_type", "by_hour", "combined_report"]


def main() -> None:
    CATALOG.unlink(missing_ok=True)
    shutil.rmtree(DATA_PATH, ignore_errors=True)

    setup = duckdb.connect()
    setup.execute("INSTALL ducklake; INSTALL sqlite; LOAD ducklake;")
    setup.execute(f"ATTACH 'ducklake:sqlite:{CATALOG}' AS dl (DATA_PATH '{DATA_PATH}/')")
    setup.execute("CREATE TABLE dl.task_runs (task_name VARCHAR, status VARCHAR)")
    setup.close()

    print(f"dispatching {len(TASKS)} workers concurrently against one DuckLake catalog...")
    procs = [
        subprocess.Popen([sys.executable, str(HERE / "worker.py"), str(CATALOG), str(DATA_PATH), t])
        for t in TASKS
    ]
    for p, t in zip(procs, TASKS, strict=True):
        if p.wait() != 0:
            raise RuntimeError(f"worker for {t!r} exited with code {p.returncode}")

    # No merge step: every commit already landed in the one shared table.
    con = duckdb.connect()
    con.execute("INSTALL ducklake; INSTALL sqlite; LOAD ducklake;")
    con.execute(f"ATTACH 'ducklake:sqlite:{CATALOG}' AS dl (DATA_PATH '{DATA_PATH}/')")
    print("\nfinal state (queried directly, no absorb/compact needed):")
    print(con.execute("SELECT * FROM dl.task_runs ORDER BY task_name").fetchall())
    con.close()


if __name__ == "__main__":
    main()
