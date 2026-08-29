"""Build a DAG from a pipeline module.

A pipeline is just a Python module (DESIGN.md sec 5); DuckPipe discovers
its tasks by importing it once and scanning the module namespace for
``Task`` instances -- module-level attributes directly, and one level into
list/tuple/set/dict values, which is where the "plain Python loop
generating uniquely identified task instances" fan-out pattern (sec 4)
naturally puts its tasks.
"""

from __future__ import annotations

import importlib.util
import re
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


def _package_context(path: Path) -> tuple[Path, str] | None:
    """If ``path`` sits inside a real Python package (an ``__init__.py``
    beside it, and so on up the tree), return ``(root_parent, dotted_name)``
    so it can be imported through the normal import system -- the only way
    a relative import between sibling task files (``from .extract import
    extract``) resolves correctly. Returns ``None`` for a standalone
    script (no ``__init__.py`` beside it) -- the common case, and the one
    ``load_module`` leaves entirely unchanged.
    """
    if not (path.parent / "__init__.py").exists():
        return None
    parts = [] if path.name == "__init__.py" else [path.stem]
    current = path.parent
    while (current / "__init__.py").exists():
        parts.append(current.name)
        current = current.parent
    parts.reverse()
    return current, ".".join(parts)


def load_module(path: str | Path) -> ModuleType:
    """Import a pipeline file. A plain standalone script (no adjacent
    ``__init__.py``) always loads fresh, under a unique synthetic name --
    calling this twice on the same edited-in-place file never serves a
    stale cached version. A file that's a real package member instead
    goes through ``importlib.import_module`` so relative imports between
    sibling task-definition files resolve exactly like any other Python
    package -- ordinary ``sys.modules`` caching then applies too, the
    same as importing that package any other way. Splitting a pipeline's
    tasks across multiple files needs no DuckPipe-specific mechanism
    either way (DESIGN.md tenet #3): it's just Python composition, with
    one entrypoint module DuckPipe is pointed at.
    """
    path = Path(path).resolve()
    package = _package_context(path)
    if package is not None:
        root_parent, dotted_name = package
        root_parent_str = str(root_parent)
        if root_parent_str not in sys.path:
            sys.path.insert(0, root_parent_str)
        return importlib.import_module(dotted_name)

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


# --- Rendering / serialization -------------------------------------------
# Both used by `duckpipe show`, but public here too: a notebook, a custom
# dashboard, or a CI comment step wants the same DAG-as-Mermaid/-JSON
# without shelling out to the CLI as a subprocess just to get a string.

_MERMAID_STATUS_CLASS = {
    "success": "success",
    "skipped": "skipped",
    "failed": "failed",
    "upstream_failed": "failed",
}
_MERMAID_CLASS_DEFS = {
    "success": "classDef success fill:#d4f7dc,stroke:#2f9e44,color:#1a1a1a",
    "skipped": "classDef skipped fill:#fff3cd,stroke:#d9a400,color:#1a1a1a",
    "failed": "classDef failed fill:#f8d7da,stroke:#d64545,color:#1a1a1a",
}


def _mermaid_node_id(name: str) -> str:
    # Task names are usually valid identifiers already (a function's own
    # __name__, or an explicit name=...), but a Mermaid node id has its
    # own, stricter rules -- sanitize rather than assume, so an unusual
    # name= never produces a broken diagram.
    return "t_" + re.sub(r"[^A-Za-z0-9_]", "_", name)


def to_mermaid(
    order: list[Task], last_status: dict[str, tuple[str, str]] | None = None
) -> str:
    """A `flowchart TD` of a task's real dependency edges, one node per
    task (labeled with its real name, not the sanitized id), colored by
    ``last_status`` when given -- pastes straight into any Markdown that
    renders Mermaid (GitHub, GitLab, many others), or into a notebook
    cell via ``IPython.display.Markdown``.

    ``last_status`` maps a task name to ``(status, timestamp)``, the same
    shape ``StateStore.last_status`` returns -- omit it for a plain,
    uncolored diagram of the DAG's shape alone.
    """
    last_status = last_status or {}
    lines = ["flowchart TD"]
    for t in order:
        label = t.name.replace('"', "&quot;")
        lines.append(f'    {_mermaid_node_id(t.name)}["{label}"]')
    for t in order:
        for up in t.upstream_tasks():
            lines.append(f"    {_mermaid_node_id(up.name)} --> {_mermaid_node_id(t.name)}")

    used_classes: list[str] = []
    for t in order:
        status = last_status.get(t.name, (None, None))[0]
        cls = _MERMAID_STATUS_CLASS.get(status)
        if cls:
            lines.append(f"    class {_mermaid_node_id(t.name)} {cls}")
            if cls not in used_classes:
                used_classes.append(cls)
    for cls in used_classes:
        lines.append(f"    {_MERMAID_CLASS_DEFS[cls]}")
    return "\n".join(lines)


def to_json(
    order: list[Task], next_run: dict[str, str] | None = None
) -> list[dict[str, object]]:
    """Topological order plus dependency edges, as plain JSON-serializable
    dicts -- the discovery primitive a coordinator dispatching
    `--only <task>` calls needs (DESIGN.md sec 8). Pass ``next_run`` (task
    name -> a human-readable preview string, e.g. from `would_skip`) to
    include it per task; omitted entirely when not given, rather than
    guessed at."""
    next_run = next_run or {}
    result: list[dict[str, object]] = []
    for t in order:
        entry: dict[str, object] = {
            "task": t.name,
            "depends_on": sorted(u.name for u in t.upstream_tasks()),
        }
        if t.name in next_run:
            entry["next_run"] = next_run[t.name]
        result.append(entry)
    return result
