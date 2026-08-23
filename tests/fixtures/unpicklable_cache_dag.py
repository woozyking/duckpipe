"""A task that succeeds but returns something that can't be pickled --
e.g. a live database handle -- must not crash the run just because
cache=True couldn't persist it (see duckpipe/scheduler.py)."""

import sqlite3

from duckpipe import task


@task(cache=True)
def opens_a_live_handle():
    return sqlite3.connect(":memory:")
