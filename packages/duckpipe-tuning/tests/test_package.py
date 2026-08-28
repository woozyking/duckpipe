"""Cross-cutting invariant: every engine's tuning module is host-spec-only
-- importing it must never pull in that engine's own (often heavy)
library. Data-blindness is enforced structurally, not just by convention.
"""

import sys

import pytest

_ENGINE_MODULES = {
    "duckpipe_tuning.duckdb": "duckdb",
    "duckpipe_tuning.polars": "polars",
    "duckpipe_tuning.dask": "dask",
    "duckpipe_tuning.daft": "daft",
}


@pytest.mark.parametrize(("tuning_module", "engine_module"), _ENGINE_MODULES.items())
def test_tuning_module_never_imports_its_own_engine(tuning_module, engine_module):
    __import__(tuning_module)
    assert engine_module not in sys.modules


def test_top_level_reexports_are_backward_compatible():
    import duckpipe_tuning

    assert duckpipe_tuning.suggest_thread_count is not None
    assert duckpipe_tuning.suggest_duckdb_settings is not None
    assert duckpipe_tuning.suggest_temp_dir_limit is not None
