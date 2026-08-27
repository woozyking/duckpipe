"""A pipeline whose tasks live in sibling files (extract.py/transform.py),
wired together via a normal relative import -- this file is the one
entrypoint DuckPipe is pointed at, per DESIGN.md tenet #3: composing
tasks across files needs no DuckPipe-specific mechanism, just Python.
"""

from .transform import transform  # noqa: F401 -- discovered via vars(), not by name

if __name__ == "__main__":
    from duckpipe import run

    run(__file__)
