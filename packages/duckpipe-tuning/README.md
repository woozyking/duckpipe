# duckpipe-tuning

Optional, host-spec-based tuning suggestions for tasks that use a data
engine directly — one module per engine, each a pure function of host
specs (CPU count, total RAM, free disk space). None of them touch a
connection, run a query, or look at your data — that's the boundary
this package holds itself to (DESIGN.md §12), so it can never grow into
the engine-aware core `duckpipe` deliberately avoids.

This is deliberately a **separate package** from `duckpipe` itself
(DESIGN.md §6.1): the core orchestrator never imports `psutil` or knows
anything about any engine's tuning knobs. Install this only if you want
it:

```bash
uv add duckpipe-tuning
```

## DuckDB

```python
import duckdb
from duckpipe_tuning.duckdb import suggest_duckdb_settings

con = duckdb.connect()
settings = suggest_duckdb_settings()
con.execute(f"SET threads = {settings['threads']}")
con.execute(f"SET memory_limit = '{settings['memory_limit']}'")
```

`suggest_thread_count()`, `suggest_duckdb_settings(mem_fraction=, workload=, threads=)`,
`suggest_temp_dir_limit(path, free_fraction=)`.

## Polars

`POLARS_MAX_THREADS` is read once, at import time — apply this *before*
`import polars`:

```python
import os
from duckpipe_tuning.polars import suggest_polars_settings

os.environ["POLARS_MAX_THREADS"] = str(suggest_polars_settings()["POLARS_MAX_THREADS"])
import polars as pl
```

## Dask

Mirrors `LocalCluster`'s own default `n_workers`/`threads_per_worker`
heuristic, but makes the numbers explicit and lets a `mem_fraction`
budget replace Dask's own `memory_limit="auto"` (which otherwise claims
the entire host):

```python
from distributed import Client
from duckpipe_tuning.dask import suggest_dask_settings

settings = suggest_dask_settings()
client = Client(**settings)
```

## Daft

```python
import daft
from duckpipe_tuning.daft import suggest_num_threads

daft.context.set_runner_native(num_threads=suggest_num_threads())
```

Daft's native runner defaults to every visible core, which measurably
*hurts* throughput on many-core hosts (the scheduler starts contending
with itself); `suggest_num_threads()` caps it.

## Pandas

No tuning surface here on purpose — pandas has no engine-level thread
count or memory ceiling to configure, so there's nothing this package
could suggest without inventing a knob pandas itself doesn't have.

## Backward compatibility

`suggest_thread_count`, `suggest_duckdb_settings`, and
`suggest_temp_dir_limit` are still importable from the top-level
`duckpipe_tuning` package (DuckDB was the only engine before 0.2.0).
New code should import from `duckpipe_tuning.duckdb` directly, the same
as every other engine's module.
