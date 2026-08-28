"""Host-spec-based tuning suggestions for Polars.

Same boundary as `duckpipe_tuning.duckdb`: a pure function of host specs
only, never of a DataFrame/LazyFrame or your data.
"""

from __future__ import annotations

import psutil

__all__ = ["suggest_thread_count", "suggest_polars_settings"]


def suggest_thread_count() -> int:
    """Logical core count -- Polars' own default. Unlike DuckDB's
    morsel-driven engine (which loses throughput to SMT over-launch),
    Polars' work-stealing thread pool is designed to use every logical
    core, hyperthreading included."""
    return psutil.cpu_count(logical=True) or psutil.cpu_count(logical=False) or 1


def suggest_polars_settings(*, threads: int | None = None) -> dict[str, object]:
    """Suggest a ``POLARS_MAX_THREADS`` value.

    Polars sizes its global thread pool once, from this environment
    variable, the moment it's imported -- so apply this *before*
    ``import polars``, not after::

        import os
        from duckpipe_tuning.polars import suggest_polars_settings

        os.environ["POLARS_MAX_THREADS"] = str(suggest_polars_settings()["POLARS_MAX_THREADS"])
        import polars as pl
    """
    threads = threads if threads is not None else suggest_thread_count()
    return {"POLARS_MAX_THREADS": threads}
