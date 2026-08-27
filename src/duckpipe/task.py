"""Task definitions and dependency inference.

Dependency inference rule (see DESIGN.md open question #1, now resolved):
an upstream dependency is declared by using the upstream ``Task`` object
itself as a parameter's *default value*. Python evaluates default values
once, at function-definition time, so ``def b(x=a): ...`` binds ``x``'s
default to the ``Task`` object ``a`` -- no separate graph-building API, no
decorator argument, and no type-hint parsing (which would be ambiguous
whenever two parameters share a type). At DAG-build time DuckPipe inspects
each task's signature, finds parameters whose default is a ``Task``, and
treats those as edges; at run time it replaces that default with the
upstream task's actual result.

This rule also settles the ``*args``/``**kwargs`` edge case cleanly: Python
syntax never allows a default value on ``*args``/``**kwargs``, so those
parameters are simply never eligible for inference -- not a special case
to handle, a consequence of the mechanism itself.

Tasks that don't accept upstream data via arguments (side-effect-only
steps) can still declare an edge with the explicit ``depends_on=[...]``
escape hatch.
"""

from __future__ import annotations

import inspect
from collections.abc import Callable
from typing import Any

from duckpipe.fingerprint import fingerprint_source


class Task:
    """Wraps a plain Python function as a DAG node.

    Calling a ``Task`` directly (``my_task(...)``) runs the underlying
    function immediately, with no DAG/state involved -- tasks stay unit
    testable as plain functions.
    """

    def __init__(
        self,
        func: Callable[..., Any],
        *,
        name: str | None = None,
        retries: int = 0,
        retry_delay: float = 0.0,
        cache: bool = False,
        cache_backend: str = "pickle",
        depends_on: list[Task] | None = None,
        extra_fingerprint: list[Any] | None = None,
    ) -> None:
        self.func = func
        self.name = name or func.__name__
        self.retries = retries
        self.retry_delay = retry_delay
        self.cache = cache
        self.cache_backend = cache_backend
        self.depends_on = list(depends_on or [])
        self.extra_fingerprint = list(extra_fingerprint or [])
        self.signature = inspect.signature(func)
        self.source_fingerprint = fingerprint_source(func)
        self.__name__ = self.name
        self.__doc__ = func.__doc__

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return self.func(*args, **kwargs)

    def __repr__(self) -> str:
        return f"Task({self.name!r})"

    def upstream_params(self) -> dict[str, Task]:
        """Parameters whose default value is another ``Task`` -- the inferred edges."""
        edges: dict[str, Task] = {}
        for pname, param in self.signature.parameters.items():
            if isinstance(param.default, Task):
                edges[pname] = param.default
        return edges

    def upstream_tasks(self) -> list[Task]:
        seen: dict[str, Task] = {}
        for t in self.upstream_params().values():
            seen[t.name] = t
        for t in self.depends_on:
            seen[t.name] = t
        return list(seen.values())


def task(
    func: Callable[..., Any] | None = None,
    *,
    name: str | None = None,
    retries: int = 0,
    retry_delay: float = 0.0,
    cache: bool = False,
    cache_backend: str = "pickle",
    depends_on: list[Task] | None = None,
    extra_fingerprint: list[Any] | None = None,
) -> Task | Callable[[Callable[..., Any]], Task]:
    """Decorate a plain Python function as a DuckPipe task.

    Usage::

        @task
        def extract():
            return duckdb.sql("select * from read_parquet('data.parquet')")

        @task(retries=3, cache=True)
        def transform(rel=extract):  # `rel=extract` infers the edge
            return rel.filter("amount > 0")
    """

    def wrap(fn: Callable[..., Any]) -> Task:
        return Task(
            fn,
            name=name,
            retries=retries,
            retry_delay=retry_delay,
            cache=cache,
            cache_backend=cache_backend,
            depends_on=depends_on,
            extra_fingerprint=extra_fingerprint,
        )

    if func is not None:
        return wrap(func)
    return wrap
