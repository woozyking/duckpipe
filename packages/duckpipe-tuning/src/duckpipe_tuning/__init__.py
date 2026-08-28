"""Host-spec-based tuning suggestions, one module per engine (DESIGN.md §6.1, §7).

Every function anywhere in this package is a pure function of the *host*
-- CPU count, total RAM, free disk space -- never of a connection, a
query, or your data. That's a deliberate, load-bearing boundary (§12):
the moment a helper here inspects a query or a connection, it has become
the engine-aware core `duckpipe` itself refuses to be, just relocated.

Call these from your own task code and apply the result to your own
engine -- nothing in `duckpipe` imports this package or knows it exists.

    from duckpipe_tuning.duckdb import suggest_duckdb_settings
    from duckpipe_tuning.polars import suggest_polars_settings
    from duckpipe_tuning.dask import suggest_dask_settings
    from duckpipe_tuning.daft import suggest_num_threads

The three original top-level names re-exported below are DuckDB's --
kept for backward compatibility with the pre-multi-engine API. New code,
and any second engine, should import from its own submodule instead.
"""

from __future__ import annotations

from duckpipe_tuning.duckdb import (
    suggest_duckdb_settings,
    suggest_temp_dir_limit,
    suggest_thread_count,
)

__all__ = ["suggest_thread_count", "suggest_duckdb_settings", "suggest_temp_dir_limit"]
__version__ = "0.2.0"
