"""End-to-end checks for the in-repo examples (ROADMAP.md Phase 1: "build
3-5 realistic example pipelines... have someone unfamiliar with the
project try them cold"). These run entirely against the bundled NYC TLC
taxi sample -- no network required -- so they're as fast and hermetic as
the rest of the suite.
"""

import shutil
from pathlib import Path

from duckpipe.scheduler import run

EXAMPLES = Path(__file__).parent.parent / "examples"


def test_daily_batch_etl_example(tmp_path):
    pipeline = EXAMPLES / "01_daily_batch_etl" / "pipeline.py"
    warehouse = pipeline.parent / "warehouse.duckdb"
    try:
        summary = run(pipeline, db_path=tmp_path / "state.duckdb")
        assert summary.success
        assert summary.results["load"] > 0
        assert warehouse.exists()
    finally:
        warehouse.unlink(missing_ok=True)


def test_fanout_partitions_example(tmp_path):
    pipeline = EXAMPLES / "02_fanout_partitions" / "pipeline.py"
    output_dir = pipeline.parent / "output"
    try:
        summary = run(pipeline, db_path=tmp_path / "state.duckdb")
        assert summary.success
        combine = summary.results["combine"]
        assert combine["partitions"] == len(combine["boroughs"])
        assert output_dir.exists()
        assert len(list(output_dir.glob("*.parquet"))) == combine["partitions"]
    finally:
        shutil.rmtree(output_dir, ignore_errors=True)


def test_incremental_sql_chain_example(tmp_path):
    pipeline = EXAMPLES / "03_incremental_sql_chain" / "pipeline.py"
    report = pipeline.parent / "report.csv"
    try:
        db_path = tmp_path / "state.duckdb"
        first = run(pipeline, db_path=db_path)
        assert first.success
        assert report.exists()

        second = run(pipeline, db_path=db_path)
        assert all(status == "skipped" for status in second.statuses.values())
    finally:
        report.unlink(missing_ok=True)
