"""Host-spec-based tuning suggestions for Dask.

Same boundary as `duckpipe_tuning.duckdb`: a pure function of host specs
only, never of a Client/LocalCluster or your data.

``suggest_worker_topology`` mirrors ``LocalCluster``'s own default
``n_workers``/``threads_per_worker`` heuristic
(``distributed.deploy.utils.nprocesses_nthreads``) instead of inventing a
new one: <=4 cores gets one process per core (avoids GIL contention on a
small box); above that, it balances toward the smallest worker count
whose thread count doesn't exceed it (both land near sqrt(cores)), since
a process-per-core split would carry needless per-process memory/import
overhead at higher core counts.
"""

from __future__ import annotations

import math

import psutil

__all__ = ["suggest_worker_topology", "suggest_dask_settings"]

_GIB = 1024**3


def suggest_worker_topology(*, cores: int | None = None) -> tuple[int, int]:
    """``(n_workers, threads_per_worker)`` for a `LocalCluster`/`Client`."""
    n = cores if cores is not None else (psutil.cpu_count(logical=True) or 1)
    if n <= 4:
        return n, 1
    n_workers = min(f for f in range(1, n + 1) if n % f == 0 and f >= math.sqrt(n))
    return n_workers, max(1, n // n_workers)


def suggest_dask_settings(
    *, mem_fraction: float = 0.8, cores: int | None = None
) -> dict[str, object]:
    """Suggest ``n_workers``/``threads_per_worker``/``memory_limit`` for a
    `distributed.Client`/`LocalCluster`.

    Dask's own ``memory_limit="auto"`` splits the *entire* host's RAM
    across workers with no fraction held back -- fine in a dedicated
    cluster node, less fine when Dask is one engine among several
    sharing a box. This makes the number explicit and adjustable via
    ``mem_fraction``, the same knob `duckpipe_tuning.duckdb` already
    uses.
    """
    n_workers, threads_per_worker = suggest_worker_topology(cores=cores)
    total_bytes = psutil.virtual_memory().total
    per_worker_bytes = int(total_bytes * mem_fraction) // n_workers
    per_worker_gb = max(1, per_worker_bytes // _GIB)
    return {
        "n_workers": n_workers,
        "threads_per_worker": threads_per_worker,
        "memory_limit": f"{per_worker_gb}GB",
    }
