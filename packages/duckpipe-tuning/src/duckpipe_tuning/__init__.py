"""Host-spec-based tuning suggestions for DuckDB (DESIGN.md §6.1, §7).

Every function here is a pure function of the *host* -- CPU count, total
RAM, free disk space -- never of a DuckDB connection, a query, or your
data. That's a deliberate, load-bearing boundary (§12):
the moment a helper here inspects a query or a connection, it has become
the engine-aware core `duckpipe` itself refuses to be, just relocated.

Call these from your own task code and apply the result to your own
connection -- nothing in `duckpipe` imports this package or knows it
exists.
"""

from __future__ import annotations

import shutil
from pathlib import Path

import psutil

__all__ = ["suggest_thread_count", "suggest_duckdb_settings", "suggest_temp_dir_limit"]
__version__ = "0.1.0"

# DuckDB's own guidance: ~1-2GB/thread for aggregation-heavy workloads,
# ~3-4GB/thread for join-heavy ones (DESIGN.md §7, §13).
_MEMORY_PER_THREAD_GB = {
    "aggregation": 1.5,
    "join": 3.5,
    "balanced": 2.0,
}

_GIB = 1024**3


def suggest_thread_count(*, hyperthreading_discount: bool = True) -> int:
    """Physical core count by default -- DuckDB's morsel-driven parallelism
    can lose throughput to SMT/hyperthreading over-launch (§7). Pass
    ``hyperthreading_discount=False`` for I/O/remote-scan-heavy workloads,
    where DuckDB's own docs suggest logical-core-based oversubscription
    instead.
    """
    physical = psutil.cpu_count(logical=False)
    logical = psutil.cpu_count(logical=True)
    if hyperthreading_discount:
        return physical or logical or 1
    return logical or physical or 1


def suggest_duckdb_settings(
    *,
    mem_fraction: float = 0.8,
    workload: str = "balanced",
    threads: int | None = None,
) -> dict[str, object]:
    """Suggest ``threads``/``memory_limit`` values for ``SET`` statements.

    ``workload`` is one of ``"aggregation"``, ``"join"``, or
    ``"balanced"`` and only affects the per-thread memory ceiling this
    applies on top of ``mem_fraction`` of total RAM -- whichever is
    smaller wins, so a many-threaded, join-heavy host doesn't get handed
    an unrealistically large `memory_limit`.
    """
    threads = threads if threads is not None else suggest_thread_count()
    total_bytes = psutil.virtual_memory().total
    fraction_budget = int(total_bytes * mem_fraction)

    per_thread_gb = _MEMORY_PER_THREAD_GB.get(workload, _MEMORY_PER_THREAD_GB["balanced"])
    per_thread_budget = int(per_thread_gb * _GIB * threads)

    memory_limit_bytes = min(fraction_budget, per_thread_budget)
    memory_limit_gb = max(1, memory_limit_bytes // _GIB)

    return {
        "threads": threads,
        "memory_limit": f"{memory_limit_gb}GB",
    }


def suggest_temp_dir_limit(path: str | Path = ".", *, free_fraction: float = 0.9) -> str:
    """Suggest a ``max_temp_directory_size`` value: a fraction of free disk
    space on the volume containing ``path`` (default ~90%, matching
    DuckDB's own default -- §7, §13)."""
    usage = shutil.disk_usage(path)
    limit_gb = max(1, int(usage.free * free_fraction) // _GIB)
    return f"{limit_gb}GB"
