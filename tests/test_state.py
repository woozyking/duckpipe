from duckpipe.state import StateStore


def test_pipeline_run_lifecycle(tmp_path):
    with StateStore(tmp_path / "state.duckdb") as store:
        run_id = store.start_run("pipeline.py")
        store.finish_run(run_id, "success")
        row = store.con.execute(
            "SELECT status FROM pipeline_runs WHERE run_id = ?", [run_id]
        ).fetchone()
        assert row == ("success",)


def test_fingerprint_roundtrip(tmp_path):
    with StateStore(tmp_path / "state.duckdb") as store:
        assert store.get_fingerprint("t") is None
        store.set_fingerprint("t", "abc123")
        assert store.get_fingerprint("t") == "abc123"
        store.set_fingerprint("t", "def456")  # upsert
        assert store.get_fingerprint("t") == "def456"


def test_cache_roundtrip_with_arbitrary_python_object(tmp_path):
    with StateStore(tmp_path / "state.duckdb") as store:
        assert not store.has_cached("t", "fp1")
        store.set_cached("t", "fp1", {"a": [1, 2, 3]})
        assert store.has_cached("t", "fp1")
        assert store.get_cached("t", "fp1") == {"a": [1, 2, 3]}


def test_arrow_cache_backend_roundtrips_a_polars_dataframe(tmp_path):
    import polars as pl

    df = pl.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
    with StateStore(tmp_path / "state.duckdb") as store:
        store.set_cached("t", "fp1", df, backend="arrow")
        result = store.get_cached("t", "fp1")

    # A cache hit under the arrow backend always hands back a plain
    # pyarrow.Table (ROADMAP.md sec 6.2) regardless of the original type.
    assert pl.from_arrow(result).equals(df)


def test_arrow_cache_backend_roundtrips_a_duckdb_relation(tmp_path):
    import duckdb
    import pyarrow as pa

    rel = duckdb.sql("SELECT 1 AS x, 'a' AS y UNION ALL SELECT 2, 'b'")
    with StateStore(tmp_path / "state.duckdb") as store:
        store.set_cached("t", "fp1", rel, backend="arrow")
        result = store.get_cached("t", "fp1")

    assert isinstance(result, pa.Table)
    assert result.to_pylist() == [{"x": 1, "y": "a"}, {"x": 2, "y": "b"}]


def test_unknown_cache_backend_raises_a_clear_error(tmp_path):
    import pytest

    with StateStore(tmp_path / "state.duckdb") as store, pytest.raises(ValueError, match="nope"):
        store.set_cached("t", "fp1", 123, backend="nope")


def test_lineage_replaces_on_rerecord(tmp_path):
    with StateStore(tmp_path / "state.duckdb") as store:
        store.record_lineage("b", ["a"])
        store.record_lineage("b", ["a", "c"])
        rows = store.con.execute(
            "SELECT upstream_task_name FROM task_lineage WHERE task_name = 'b'"
        ).fetchall()
        assert {r[0] for r in rows} == {"a", "c"}


def test_last_status_returns_most_recent(tmp_path):
    from datetime import UTC, datetime, timedelta

    with StateStore(tmp_path / "state.duckdb") as store:
        t0 = datetime(2026, 1, 1, tzinfo=UTC)
        t1 = t0 + timedelta(minutes=5)
        store.record_task_run("r1", "t", "failed", t0, t0, "fp1", error="boom")
        store.record_task_run("r2", "t", "success", t1, t1, "fp2")
        status, _ = store.last_status("t")
        assert status == "success"
