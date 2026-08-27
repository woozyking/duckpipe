# 05 — distributed cluster, with DuckLake

The same coordination problem as
[`../04_distributed_cluster`](../04_distributed_cluster/) — many workers
need to safely record what they did — solved with a DuckLake catalog
instead of delta files.

```bash
uv run python ducklake_cluster.py
```

**What's different from 04:** every worker commits straight into one
shared table (`INSERT INTO dl.task_runs ...`). There's no `.pending/`
directory, no absorb step, no `duckpipe compact` — the moment a worker
commits, its row is just *there*, visible to anyone else querying the
table. DuckLake's catalog (here, a local SQLite file — no external
services, fully offline) gives you that ACID guarantee directly.

**What this example is *not*:** a literal `duckpipe.run()` backend, even
though one exists now (`db_path="ducklake:..."`, DESIGN.md sec 8 — see
[`../06_ducklake_observability`](../06_ducklake_observability/)).
This example predates that and demonstrates the raw coordination
mechanism on its own terms — attaching a shared catalog directly and
committing to it — so you can see exactly what it buys independent of
whether `duckpipe.run()` itself is involved.

**The honest tradeoff**, verified directly before writing this, not
assumed: run `worker.py` concurrently against a fresh SQLite-catalog
DuckLake with no retry logic, and some commits fail outright —

```
worker 2: FAILED - TransactionException: Failed to commit DuckLake
transaction. Failed to flush changes into DuckLake: database is locked
```

— 3 of 8 concurrent writers failed in that test. `worker.py` in this
example retries past that (a few hundred milliseconds of backoff is
enough at this scale), which is a legitimate, simple fix for moderate
concurrency. If your write volume is high enough that retrying stops
being enough, the next step is a **Postgres catalog** instead of
SQLite — also verified directly (a real Postgres 17 via
`docker run postgres`): the identical 8-worker test with *no* retry
logic at all, zero failures. That's a fully supported option for
`duckpipe.run()`'s own DuckLake backend too, not just this example's
ad hoc coordination demo — see the "bonus" section of
[`../06_ducklake_observability`](../06_ducklake_observability/)'s
README. It needs a reachable Postgres instance, which is why it's an
opt-in for teams that already run one rather than this repo's default —
never required to get the SQLite version above working.

**When to actually reach for this over 04's delta-merge:** you want real
ACID guarantees on the *data* itself (not just DuckPipe's own
coordination bookkeeping), time travel, schema evolution, or other
processes — not necessarily DuckPipe-aware ones — reading the same
catalog. If all you need is "many workers can safely record what they
did," 04's mechanism already provides that with zero extra moving parts.
