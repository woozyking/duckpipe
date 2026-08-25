"""Phase 3b: the DuckLake-backed state store -- an opt-in observability
upgrade (time travel over run history, schema evolution), not a rework of
Phase 3a's distributed mechanism (ROADMAP.md sec 8). These need network on
a cold extension cache (`INSTALL ducklake`/`sqlite`), same as
examples/05_distributed_with_ducklake.
"""

from pathlib import Path

import pytest

from duckpipe.scheduler import run
from duckpipe.state import StateStore, is_ducklake

FIXTURES = Path(__file__).parent / "fixtures"


def _catalog(tmp_path: Path) -> str:
    return f"ducklake:sqlite:{tmp_path / 'pipeline.ducklake.sqlite'}"


def test_is_ducklake_detects_the_prefix():
    assert is_ducklake("ducklake:sqlite:foo.sqlite")
    assert not is_ducklake("plain/path/duckpipe.db")
    assert not is_ducklake(Path("plain/path/duckpipe.db"))


def test_run_against_ducklake_backend_and_skip_on_second_run(tmp_path):
    catalog = _catalog(tmp_path)
    first = run(FIXTURES / "toy_dag.py", db_path=catalog)
    assert first.success
    assert first.results["load"] == [2, 4, 6, 101, 102, 103]

    second = run(FIXTURES / "toy_dag.py", db_path=catalog)
    assert all(status == "skipped" for status in second.statuses.values())


def test_data_path_is_auto_derived_as_a_sibling_directory(tmp_path):
    catalog = _catalog(tmp_path)
    run(FIXTURES / "toy_dag.py", db_path=catalog)
    assert (tmp_path / "pipeline.ducklake.sqlite.data").is_dir()


def test_explicit_data_path_override(tmp_path):
    catalog = _catalog(tmp_path)
    custom_data = tmp_path / "custom_data"
    run(FIXTURES / "toy_dag.py", db_path=catalog, data_path=str(custom_data))
    assert custom_data.is_dir()
    assert not (tmp_path / "pipeline.ducklake.sqlite.data").exists()


def test_only_combined_with_ducklake_raises_clearly(tmp_path):
    with pytest.raises(ValueError, match="only="):
        run(FIXTURES / "toy_dag.py", db_path=_catalog(tmp_path), only="extract")


def test_state_uri_combined_with_ducklake_raises_clearly(tmp_path):
    with pytest.raises(ValueError, match="state_uri"):
        run(FIXTURES / "toy_dag.py", db_path=_catalog(tmp_path), state_uri="file:///tmp/x")


def test_snapshots_raises_on_a_plain_store(tmp_path):
    with StateStore(tmp_path / "plain.duckdb") as store:
        assert not store.is_ducklake
        with pytest.raises(RuntimeError, match="DuckLake"):
            store.snapshots()


def test_snapshots_are_tagged_with_readable_commit_messages(tmp_path):
    catalog = _catalog(tmp_path)
    run(FIXTURES / "toy_dag.py", db_path=catalog)

    with StateStore(catalog, read_only=True) as store:
        assert store.is_ducklake
        messages = [s["commit_message"] for s in store.snapshots() if s["commit_message"]]
    assert any("task extract succeeded" in m for m in messages)
    assert any("task load succeeded" in m for m in messages)
    assert any(m.startswith("pipeline run started") for m in messages)
    assert any(m.startswith("pipeline run finished") for m in messages)


def test_time_travel_query_sees_state_as_of_an_earlier_snapshot(tmp_path):
    catalog = _catalog(tmp_path)
    run(FIXTURES / "toy_dag.py", db_path=catalog)

    with StateStore(catalog, read_only=True) as store:
        snaps = store.snapshots()
        # The snapshot right after "extract succeeded" but before any
        # other task ran -- exactly one row in task_runs at that point.
        extract_snapshot_id = next(
            s["snapshot_id"] for s in snaps if s["commit_message"] == "task extract succeeded"
        )
        rows = store.con.execute(
            f"SELECT task_name FROM task_runs AT (VERSION => {extract_snapshot_id})"
        ).fetchall()
        assert [r[0] for r in rows] == ["extract"]


def test_read_only_open_does_not_write_schema(tmp_path):
    catalog = _catalog(tmp_path)
    run(FIXTURES / "toy_dag.py", db_path=catalog)

    with StateStore(catalog, read_only=True) as store:
        before = len(store.snapshots())
        store.last_status("extract")
    with StateStore(catalog, read_only=True) as store:
        after = len(store.snapshots())
    assert before == after
