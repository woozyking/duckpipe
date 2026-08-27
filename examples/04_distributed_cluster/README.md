# 04 — distributed cluster (no DuckLake, no Quack)

Runs `pipeline.py`'s four tasks across several worker *processes* that
never share a Python process, a connection, or a lock on the whole state
file — only the `only=`/`--only` mechanism and delta-merge state
(DESIGN.md sec 8).

```bash
uv run python run_cluster.py
```

What happens:

1. `run_cluster.py` (the coordinator — deliberately *not* part of
   DuckPipe itself, per tenet #1) calls `duckpipe show --json` to learn
   the topological order.
2. It dispatches each "level" of independent tasks as real, concurrent
   `duckpipe run pipeline.py --only <task> --state-uri file://...`
   subprocesses, and waits for the level to finish before moving on.
3. Each worker downloads the shared state, absorbs whatever earlier
   workers have contributed, decides skip-or-run for its one task, and —
   if it ran — uploads *only its own new rows* as a uniquely-keyed delta.
   No lock, no contention, even for the two workers in level 2 running
   at the exact same time.
4. `duckpipe compact` folds every delta into one canonical state file at
   the end (optional for correctness — every invocation already absorbs
   pending deltas itself — but keeps `.pending/` from growing forever if
   you never do a whole run).

Swap `STATE_URI` in `run_cluster.py` for a real `s3://`/`gs://`/`az://`
URI (with the matching `duckpipe[s3]`/`[gcs]`/`[azure]` extra installed)
and this becomes a genuine multi-machine cluster run — nothing else in
either file changes.

See [`../05_distributed_with_ducklake`](../05_distributed_with_ducklake/)
for the same problem solved with DuckLake instead, and when that upgrade
is actually worth it.
