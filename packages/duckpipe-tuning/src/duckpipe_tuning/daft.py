"""Host-spec-based tuning suggestion for Daft.

Same boundary as `duckpipe_tuning.duckdb`: a pure function of host specs
only, never of a DataFrame or your data.
"""

from __future__ import annotations

import psutil

__all__ = ["suggest_num_threads"]

# Daft's native ("Swordfish") runner defaults to every visible core, but on
# many-core hosts that default measurably *hurts* throughput -- the
# scheduler starts fighting itself well before the host's actual core
# count (see Eventual-Inc/Daft#3389). A ceiling, not a target -- the same
# "don't just hand it every core" caution duckpipe_tuning.duckdb already
# applies via its hyperthreading discount.
_MAX_USEFUL_THREADS = 32


def suggest_num_threads(*, cap: int = _MAX_USEFUL_THREADS) -> int:
    """Suggest a ``num_threads`` value for
    ``daft.context.set_runner_native(num_threads=...)``: physical core
    count, capped."""
    physical = psutil.cpu_count(logical=False) or psutil.cpu_count(logical=True) or 1
    return min(physical, cap)
