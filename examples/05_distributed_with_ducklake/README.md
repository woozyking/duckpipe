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

**What this example is *not*:** a literal `duckpipe.run()` backend.
Wiring DuckLake into DuckPipe's own state store — so `@task(cache=True)`
and fingerprint tracking transparently use it instead of a plain
`.duckdb` file — is real, separate engineering (ROADMAP.md sec 8/11,
Phase 3b), not something bolted on here. This example demonstrates the
coordination mechanism on its own terms, so you can see exactly what
that future backend would (and wouldn't) buy you.

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
being enough, the next step is a **Postgres catalog** instead of SQLite —
genuinely lock-free concurrent commits — which needs a reachable Postgres
instance this repo doesn't try to provide, since it's the one piece of
this whole example that would require standing infrastructure.

**When to actually reach for this over 04's delta-merge:** you want real
ACID guarantees on the *data* itself (not just DuckPipe's own
coordination bookkeeping), time travel, schema evolution, or other
processes — not necessarily DuckPipe-aware ones — reading the same
catalog. If all you need is "many workers can safely record what they
did," 04's mechanism already provides that with zero extra moving parts.
