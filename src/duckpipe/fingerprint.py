"""Content fingerprinting for tasks (ROADMAP.md tenet #6, open question #2).

A task's fingerprint hashes together:

- its own function source code
- its declarative config (retries, cache, cache_backend, depends_on names)
- the resolved fingerprints of its upstream tasks
- any values in ``extra_fingerprint`` the user opted to include

It deliberately never hashes a task's *return value* -- fingerprinting is
data-blind, per tenet #2. One accepted consequence (open question #2,
resolved for v1): an upstream *external* state change -- a source file
that changed on disk without any task code changing -- is invisible to
it. ``extra_fingerprint`` is the documented, opt-in escape hatch: pass
anything hashable (a file's mtime, a config dict, an API version string)
that should also invalidate the cache when it changes.
"""

from __future__ import annotations

import hashlib
import inspect
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from duckpipe.task import Task


def fingerprint_source(func: Callable[..., Any]) -> str:
    try:
        source = inspect.getsource(func)
    except (OSError, TypeError):
        # Dynamically generated function with no retrievable source (e.g.
        # built via exec/eval) -- fall back to bytecode identity.
        source = repr(func.__code__.co_code)
    return hashlib.sha256(source.encode("utf-8")).hexdigest()


def _combine(*parts: str) -> str:
    h = hashlib.sha256()
    for p in parts:
        h.update(p.encode("utf-8"))
        h.update(b"\0")
    return h.hexdigest()


def fingerprint_task(task: Task, upstream_fingerprints: dict[str, str]) -> str:
    config_repr = repr(
        (
            task.retries,
            task.cache,
            task.cache_backend,
            sorted(t.name for t in task.depends_on),
            [repr(v) for v in task.extra_fingerprint],
        )
    )
    upstream_repr = [f"{name}={fp}" for name, fp in sorted(upstream_fingerprints.items())]
    return _combine(task.source_fingerprint, config_repr, *upstream_repr)


def resolve_fingerprints(order: list[Task]) -> dict[str, str]:
    """Compute every task's fingerprint in topological order."""
    resolved: dict[str, str] = {}
    for t in order:
        upstream_fps = {up.name: resolved[up.name] for up in t.upstream_tasks()}
        resolved[t.name] = fingerprint_task(t, upstream_fps)
    return resolved
