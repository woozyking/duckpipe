# Remote execution: bigger single node

Sometimes a pipeline's bottleneck isn't "many tasks that could run in
parallel" (§8's `--only` story) — it's one job too large for the laptop
or CI runner it happens to be running on, but nowhere near needing a
multi-node cluster. The fix needs no new DuckPipe mechanism at all:
**run the exact same code on a bigger remote machine.** Tenet #5 (local
dev and production are the same code path) already covers this — "where
a task runs" was never coupled to "how you write it."

## The recipe

```bash
# ship the pipeline (and whatever local data it reads) to the box
rsync -av pipeline.py data/ user@bigbox:/opt/pipelines/daily_etl/

# run it there, exactly the command you'd run locally
ssh user@bigbox 'cd /opt/pipelines/daily_etl && uv run duckpipe run pipeline.py'

# bring the state file (and any local output) back
rsync -av user@bigbox:/opt/pipelines/daily_etl/duckpipe.db ./
```

Point `--state-uri` at durable storage instead of the last `rsync` step
if the box is itself ephemeral (a spot instance, a rented GPU box you
tear down after) — same mechanism as every other trigger in
[`triggers.md`](triggers.md), nothing specific to this recipe:

```bash
ssh user@bigbox 'cd /opt/pipelines/daily_etl && uv run duckpipe run pipeline.py --state-uri s3://my-bucket/pipelines/daily_etl/duckpipe.db'
```

That's the entire mechanism. No coordinator, no `--only`, no second
concept — one task moved to a bigger machine, in fact often the
*simplest* thing available in §8's whole extension path.

## Sizing the bigger box

The remote machine usually exists specifically because it has more
cores/RAM than what you were running on before — `duckpipe-tuning`
(§6.1, `packages/duckpipe-tuning/`) turns host specs into concrete
DuckDB settings so the pipeline actually uses them, called from inside
the task itself like anywhere else:

```python
import duckdb
from duckpipe_tuning.duckdb import suggest_duckdb_settings


@task(cache=True)
def transform():
    settings = suggest_duckdb_settings(workload="join")
    con = duckdb.connect()
    con.execute(f"SET threads = {settings['threads']}")
    con.execute(f"SET memory_limit = '{settings['memory_limit']}'")
    return con.sql("...")
```

This is the same opt-in utility used anywhere else DuckPipe runs — the
remote box isn't a special case, just a bigger set of specs to tune for.

## When this beats `--only` (§8)

Reach for this before task-scoped distributed execution when the
bottleneck is one big task rather than many independent ones: no
`state_uri` locking model, no delta files, no dispatch coordinator to
write — just a bigger machine running the pipeline you already have.
Reach for `--only` (§8) instead once the DAG has enough independent
tasks that running them concurrently, on possibly-different machines,
actually helps — the two aren't mutually exclusive: a `--only` worker
can itself run on a beefy remote node if the one task it's responsible
for is the heavy one.
