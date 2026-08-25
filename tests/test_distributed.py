"""Phase 3a: `only=`/`--only` task-scoped execution and the delta-merge
mechanism it uses instead of the whole-run lock (ROADMAP.md sec 8).
"""

from pathlib import Path

import pytest

from duckpipe.remote import StateLockedError, locked
from duckpipe.scheduler import UpstreamNotCachedError, UpstreamNotReadyError, run

FIXTURES = Path(__file__).parent / "fixtures"


def test_scoped_local_run_executes_only_that_task(tmp_path):
    db_path = tmp_path / "state.duckdb"
    summary = run(FIXTURES / "toy_dag.py", only="extract", db_path=db_path)

    assert summary.statuses == {"extract": "success"}
    assert summary.results["extract"] == [1, 2, 3]


def test_scoped_run_of_unknown_task_raises(tmp_path):
    with pytest.raises(ValueError, match="no task named"):
        run(FIXTURES / "toy_dag.py", only="nope", db_path=tmp_path / "state.duckdb")


def test_scoped_run_requires_upstream_ready(tmp_path):
    db_path = tmp_path / "state.duckdb"
    with pytest.raises(UpstreamNotReadyError, match="extract"):
        run(FIXTURES / "toy_dag.py", only="transform_a", db_path=db_path)


def test_scoped_run_requires_upstream_cache_for_data(tmp_path):
    db_path = tmp_path / "state.duckdb"
    fixture = FIXTURES / "scoped_uncached_upstream_dag.py"

    root_summary = run(fixture, only="root", db_path=db_path)
    assert root_summary.statuses == {"root": "success"}

    with pytest.raises(UpstreamNotCachedError, match="root"):
        run(fixture, only="leaf", db_path=db_path)


def test_scoped_local_chain_matches_whole_run_result(tmp_path):
    fixture = FIXTURES / "toy_dag.py"
    scoped_db = tmp_path / "scoped.duckdb"
    for task_name in ["extract", "transform_a", "transform_b", "load"]:
        summary = run(fixture, only=task_name, db_path=scoped_db)
        assert summary.statuses[task_name] == "success"
    assert summary.results["load"] == [2, 4, 6, 101, 102, 103]

    whole_db = tmp_path / "whole.duckdb"
    whole_summary = run(fixture, db_path=whole_db)
    assert whole_summary.results["load"] == summary.results["load"]


def test_scoped_remote_runs_merge_into_one_consistent_state(tmp_path):
    fixture = FIXTURES / "toy_dag.py"
    state_uri = f"file://{tmp_path / 'bucket' / 'duckpipe.db'}"
    run_id = "shared-run"

    # Four "workers" each handling one task of the diamond, in topological
    # order but never touching a shared local file -- each only uploads
    # its own tiny delta.
    for task_name in ["extract", "transform_a", "transform_b", "load"]:
        summary = run(
            fixture,
            only=task_name,
            state_uri=state_uri,
            run_id=run_id,
            db_path=tmp_path / f"scratch_{task_name}.duckdb",
        )
        assert summary.statuses[task_name] == "success"

    # A later whole-run invocation (a fresh "worker" with no local state at
    # all) downloads the merged state and finds everything already done.
    fresh_local = tmp_path / "fresh.duckdb"
    final = run(fixture, state_uri=state_uri, db_path=fresh_local)
    assert all(status == "skipped" for status in final.statuses.values())
    assert final.results["load"] == [2, 4, 6, 101, 102, 103]

    # All four scoped runs shared one run_id -- they should have merged
    # into a single pipeline_runs row, not four.
    import duckdb

    con = duckdb.connect(str(fresh_local), read_only=True)
    rows = con.execute("SELECT run_id FROM pipeline_runs WHERE run_id = ?", [run_id]).fetchall()
    con.close()
    assert len(rows) == 1


def test_scoped_remote_run_needs_no_lock(tmp_path):
    fixture = FIXTURES / "toy_dag.py"
    state_uri = f"file://{tmp_path / 'bucket' / 'duckpipe.db'}"

    # Hold the whole-run lock for the entire scoped call -- a scoped run
    # writes only a uniquely-keyed delta, never the locked state file
    # itself, so this must succeed regardless.
    with locked(state_uri):
        summary = run(
            fixture,
            only="extract",
            state_uri=state_uri,
            db_path=tmp_path / "scratch.duckdb",
        )
    assert summary.statuses == {"extract": "success"}


def test_whole_run_against_a_locked_state_uri_still_raises(tmp_path):
    # Sanity check that the distinction above is real: a *whole* run
    # against the same state_uri still respects the lock as before.
    fixture = FIXTURES / "toy_dag.py"
    state_uri = f"file://{tmp_path / 'bucket' / 'duckpipe.db'}"

    with locked(state_uri), pytest.raises(StateLockedError):
        run(fixture, state_uri=state_uri, db_path=tmp_path / "scratch.duckdb")


def test_compact_folds_pending_deltas_and_cleans_up(tmp_path):
    import fsspec

    from duckpipe.remote import compact

    fixture = FIXTURES / "toy_dag.py"
    bucket = tmp_path / "bucket"
    state_uri = f"file://{bucket / 'duckpipe.db'}"

    run(fixture, only="extract", state_uri=state_uri, db_path=tmp_path / "a.duckdb")
    run(fixture, only="transform_a", state_uri=state_uri, db_path=tmp_path / "b.duckdb")

    fs, prefix = fsspec.core.url_to_fs(str(bucket / "duckpipe.db.pending"))
    assert len(fs.ls(prefix)) == 2  # two uncompacted deltas sitting there

    compact(state_uri, db_path=tmp_path / "compact_scratch.duckdb")

    assert not fs.exists(prefix) or len(fs.ls(prefix)) == 0
    assert (bucket / "duckpipe.db").exists()

    # The now-compacted canonical file reflects both tasks' results.
    fresh = run(fixture, only="transform_a", state_uri=state_uri, db_path=tmp_path / "c.duckdb")
    assert fresh.statuses["transform_a"] == "skipped"


def test_redundant_scoped_dispatch_is_harmless(tmp_path):
    # Two "workers" both assigned the same task -- e.g. a coordinator
    # mistake, or a deliberate retry. Each downloads and absorbs pending
    # state before checking anything, so by the time the second one looks,
    # it already sees the first one's result as done and skips instead of
    # redoing the work -- better than merely "harmless."
    fixture = FIXTURES / "toy_dag.py"
    state_uri = f"file://{tmp_path / 'bucket' / 'duckpipe.db'}"

    first = run(fixture, only="extract", state_uri=state_uri, db_path=tmp_path / "a.duckdb")
    second = run(fixture, only="extract", state_uri=state_uri, db_path=tmp_path / "b.duckdb")

    assert first.statuses["extract"] == "success"
    assert second.statuses["extract"] == "skipped"
    assert first.results["extract"] == second.results["extract"] == [1, 2, 3]
