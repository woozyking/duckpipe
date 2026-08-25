"""One worker's contribution to the DuckLake demo: attach the shared
catalog and commit its own task's completion, retrying past the
occasional "database is locked" a SQLite catalog throws under real
concurrent commits (see this example's README for why that happens and
what a Postgres catalog would change).

Run indirectly, as a subprocess, by ducklake_cluster.py:
    python worker.py <catalog_path> <data_path> <task_name>
"""

import random
import sys
import time

import duckdb

catalog_path, data_path, task_name = sys.argv[1], sys.argv[2], sys.argv[3]

con = duckdb.connect()
con.execute("INSTALL ducklake; INSTALL sqlite; LOAD ducklake;")
con.execute(f"ATTACH 'ducklake:sqlite:{catalog_path}' AS dl (DATA_PATH '{data_path}/')")
con.execute("USE dl")

MAX_ATTEMPTS = 8
for attempt in range(1, MAX_ATTEMPTS + 1):
    try:
        con.execute("INSERT INTO task_runs VALUES (?, 'success')", [task_name])
        print(f"worker {task_name!r}: committed on attempt {attempt}")
        break
    except duckdb.TransactionException as exc:
        if "locked" not in str(exc).lower() or attempt == MAX_ATTEMPTS:
            raise
        time.sleep(random.uniform(0.05, 0.2) * attempt)
