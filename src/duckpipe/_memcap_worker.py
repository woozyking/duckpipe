"""Isolated subprocess entry point for a memory-capped task
(``@task(memory_limit_mb=...)``, see ``duckpipe._memcap``).

Invoked as: ``python -m duckpipe._memcap_worker <payload.pkl> <result.pkl>``

The payload is a cloudpickled ``{"func": callable, "kwargs": dict}``. On
success or a clean failure it writes a small pickled result; if the
parent's memory watchdog kills this process first, no file is written
and the parent records the cap breach itself.
"""

from __future__ import annotations

import sys


def main(payload_path: str, result_path: str) -> None:
    import cloudpickle

    with open(payload_path, "rb") as f:
        payload = cloudpickle.load(f)

    try:
        value = payload["func"](**payload["kwargs"])
        result: dict[str, object] = {"status": "ok", "value": value}
    except Exception as exc:  # engine/task error -- reported cleanly, not a crash
        result = {"status": "error", "error": f"{type(exc).__name__}: {exc}"}

    with open(result_path, "wb") as f:
        cloudpickle.dump(result, f)


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
