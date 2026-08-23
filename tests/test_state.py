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
