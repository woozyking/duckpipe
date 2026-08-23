"""Build a DAG from a pipeline module.

A pipeline is just a Python module (ROADMAP.md sec 5); DuckPipe discovers
its tasks by importing it once and scanning the module namespace for
``Task`` instances -- module-level attributes directly, and one level into
list/tuple/set/dict values, which is where the "plain Python loop
generating uniquely identified task instances" fan-out pattern (sec 4)
naturally puts its tasks.
"""

from __future__ import annotations

import importlib.util
import sys
from dataclasses import dataclass, field
from pathlib import Path
from types import ModuleType

from duckpipe.task import Task


class CycleError(RuntimeError):
    pass


class DuplicateTaskNameError(RuntimeError):
    pass


@dataclass
class DAG:
    module: ModuleType
    tasks: dict[str, Task] = field(default_factory=dict)

    def topological_order(self) -> list[Task]:
        indegree = {name: 0 for name in self.tasks}
        children: dict[str, list[str]] = {name: [] for name in self.tasks}
        for t in self.tasks.values():
            for up in t.upstream_tasks():
                indegree[t.name] += 1
                children[up.name].append(t.name)

        ready = sorted(name for name, deg in indegree.items() if deg == 0)
        order: list[Task] = []
        while ready:
            name = ready.pop(0)
            order.append(self.tasks[name])
            newly_ready = []
            for child in children[name]:
                indegree[child] -= 1
                if indegree[child] == 0:
                    newly_ready.append(child)
            ready = sorted(ready + newly_ready)

        if len(order) != len(self.tasks):
            remaining = sorted(set(self.tasks) - {t.name for t in order})
            raise CycleError(f"cycle detected among tasks: {remaining}")
        return order

    def roots(self) -> list[Task]:
        return [t for t in self.tasks.values() if not t.upstream_tasks()]


def _discover_tasks(module: ModuleType) -> dict[str, Task]:
    found: dict[str, Task] = {}

    def add(t: Task) -> None:
        if t.name in found and found[t.name] is not t:
            raise DuplicateTaskNameError(
                f"two distinct tasks are both named {t.name!r}; "
                "give one an explicit name=... to disambiguate"
            )
        found[t.name] = t

    for value in vars(module).values():
        if isinstance(value, Task):
            add(value)
        elif isinstance(value, list | tuple | set):
            for item in value:
                if isinstance(item, Task):
                    add(item)
        elif isinstance(value, dict):
            for item in value.values():
                if isinstance(item, Task):
                    add(item)

    return found


def load_module(path: str | Path) -> ModuleType:
    path = Path(path).resolve()
    module_name = f"duckpipe_pipeline_{path.stem}_{abs(hash(str(path)))}"
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise ImportError(f"could not load pipeline module from {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def build_dag(source: str | Path | ModuleType) -> DAG:
    """Import (if needed) a pipeline and resolve its task graph.

    Raises ``CycleError`` immediately if the graph is malformed, so callers
    never have to discover that mid-run.
    """
    module = source if isinstance(source, ModuleType) else load_module(source)
    tasks = _discover_tasks(module)

    # A task may reference an upstream Task that isn't itself bound to a
    # module-level name (e.g. constructed inline) -- pull those in too so
    # topological_order() sees the full graph.
    frontier = list(tasks.values())
    while frontier:
        t = frontier.pop()
        for up in t.upstream_tasks():
            if up.name not in tasks:
                tasks[up.name] = up
                frontier.append(up)

    dag = DAG(module=module, tasks=tasks)
    dag.topological_order()  # raises CycleError early if malformed
    return dag
