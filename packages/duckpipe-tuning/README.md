# duckpipe-tuning

Optional, host-spec-based tuning suggestions for tasks that use DuckDB
directly — `suggest_thread_count()`, `suggest_duckdb_settings()`,
`suggest_temp_dir_limit()`.

This is deliberately a **separate package** from `duckpipe` itself
(DESIGN.md §6.1): the core orchestrator never imports `psutil` or knows
anything about DuckDB's tuning knobs. Install this only if you want it:

```bash
uv add duckpipe-tuning
```

```python
import duckdb
from duckpipe_tuning import suggest_duckdb_settings

con = duckdb.connect()
settings = suggest_duckdb_settings()
con.execute(f"SET threads = {settings['threads']}")
con.execute(f"SET memory_limit = '{settings['memory_limit']}'")
```

Every function here is a pure function of **host specs only** — CPU
count, total RAM, free disk space. None of them touch a DuckDB
connection, run a query, or look at your data; that's the boundary this
package holds itself to (DESIGN.md §12), so it can
never grow into the engine-aware core `duckpipe` deliberately avoids.
