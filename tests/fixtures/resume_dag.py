"""Partial-DAG resume fixture (ROADMAP.md Phase 2): `boom` fails as long as
a test-controlled marker file exists, letting a test simulate "the run
failed partway, someone fixed the bug, re-run the same command" and
check that the already-succeeded, cache=True upstream task is reused
rather than re-executed."""

import os
from pathlib import Path

from duckpipe import task

_marker_path = os.environ.get("DUCKPIPE_RESUME_TEST_MARKER")
MARKER = Path(_marker_path) if _marker_path else None


@task(cache=True)
def root():
    return 1


@task(cache=True)
def boom(x=root):
    if MARKER is not None and MARKER.exists():
        raise ValueError("still broken")
    return x + 1


@task(cache=True)
def downstream(x=boom):
    return x * 10
