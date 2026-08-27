"""End-to-end fixture for the optional Arrow cache backend (DESIGN.md
sec 6.2): `stage` returns a Polars DataFrame and caches it via
`cache_backend="arrow"` instead of the pickle default."""

import polars as pl

from duckpipe import task


@task(cache=True, cache_backend="arrow")
def stage():
    return pl.DataFrame({"a": [1, 2, 3]})
