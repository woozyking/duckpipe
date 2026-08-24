"""End-to-end checks for the in-repo examples (ROADMAP.md Phase 1: "build
3-5 realistic example pipelines... have someone unfamiliar with the
project try them cold"). These run entirely against the bundled NYC TLC
taxi sample -- no network required -- so they're as fast and hermetic as
the rest of the suite.
"""

import shutil
from pathlib import Path

import pytest

from duckpipe.scheduler import run

EXAMPLES = Path(__file__).parent.parent / "examples"


@pytest.mark.parametrize("engine", ["duck", "pl"])
def test_daily_batch_etl_example(tmp_path, engine):
    pipeline = EXAMPLES / "01_daily_batch_etl" / f"{engine}.py"
    warehouse_ext = "duckdb" if engine == "duck" else "parquet"
    warehouse = pipeline.parent / f"warehouse.{engine}.{warehouse_ext}"
    try:
        summary = run(pipeline, db_path=tmp_path / "state.duckdb")
        assert summary.success
        assert summary.results["load"] > 0
        assert warehouse.exists()
    finally:
        warehouse.unlink(missing_ok=True)


@pytest.mark.parametrize("engine", ["duck", "pl"])
def test_fanout_partitions_example(tmp_path, engine):
    pipeline = EXAMPLES / "02_fanout_partitions" / f"{engine}.py"
    output_dir = pipeline.parent / "output" / engine
    try:
        summary = run(pipeline, db_path=tmp_path / "state.duckdb")
        assert summary.success
        combine = summary.results["combine"]
        assert combine["partitions"] == len(combine["boroughs"])
        assert output_dir.exists()
        assert len(list(output_dir.glob("*.parquet"))) == combine["partitions"]
    finally:
        shutil.rmtree(output_dir.parent, ignore_errors=True)


def test_mid_pipeline_materialization_example(tmp_path):
    pipeline = EXAMPLES / "03_mid_pipeline_materialization" / "pipeline.py"
    report = pipeline.parent / "report.csv"
    try:
        db_path = tmp_path / "state.duckdb"
        first = run(pipeline, db_path=db_path)
        assert first.success
        assert report.exists()

        second = run(pipeline, db_path=db_path)
        assert second.success
        # Only the one full-file-scan task is worth caching (see the
        # module docstring); the cheap downstream tasks are deliberately
        # left uncached, so they re-run even though nothing changed.
        assert second.statuses["daily_revenue"] == "skipped"
        assert second.statuses["rolling_revenue"] == "success"
        assert second.statuses["report"] == "success"
    finally:
        report.unlink(missing_ok=True)
