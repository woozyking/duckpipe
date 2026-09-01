import json
from pathlib import Path

from duckpipe.scheduler import run

FIXTURES = Path(__file__).parent / "fixtures"


def test_toy_dag_runs_end_to_end(tmp_path):
    summary = run(FIXTURES / "toy_dag.py", db_path=tmp_path / "state.duckdb")

    assert summary.success
    assert summary.statuses == {
        "extract": "success",
        "transform_a": "success",
        "transform_b": "success",
        "load": "success",
    }
    assert summary.results["load"] == [2, 4, 6, 101, 102, 103]


def test_second_run_skips_unchanged_tasks(tmp_path):
    db_path = tmp_path / "state.duckdb"
    run(FIXTURES / "toy_dag.py", db_path=db_path)
    second = run(FIXTURES / "toy_dag.py", db_path=db_path)

    assert all(status == "skipped" for status in second.statuses.values())
    assert second.results["load"] == [2, 4, 6, 101, 102, 103]


def test_force_reruns_everything_even_when_unchanged(tmp_path):
    db_path = tmp_path / "state.duckdb"
    run(FIXTURES / "toy_dag.py", db_path=db_path)
    second = run(FIXTURES / "toy_dag.py", db_path=db_path, force=True)

    assert all(status == "success" for status in second.statuses.values())


def test_failure_cascades_to_downstream_but_not_to_siblings(tmp_path):
    db_path = tmp_path / "state.duckdb"
    summary = run(FIXTURES / "failing_dag.py", db_path=db_path)

    assert not summary.success
    assert summary.statuses["ok_root"] == "success"
    assert summary.statuses["boom"] == "failed"
    assert summary.statuses["downstream"] == "upstream_failed"
    assert summary.statuses["sibling"] == "success"
    assert summary.results["sibling"] == 10

    # A cascade-skipped task still gets a task_runs row -- it never ran,
    # but that's itself an observable fact (DESIGN.md Phase 2: partial-DAG
    # resume relies on `task_runs`/fingerprints accurately reflecting what
    # happened, not silently omitting cascade failures).
    import duckdb

    con = duckdb.connect(str(db_path), read_only=True)
    row = con.execute("SELECT status FROM task_runs WHERE task_name = 'downstream'").fetchone()
    con.close()
    assert row == ("upstream_failed",)


def test_partial_dag_resume_reuses_cached_upstream_and_reruns_the_rest(tmp_path, monkeypatch):
    marker = tmp_path / "break"
    marker.touch()
    monkeypatch.setenv("DUCKPIPE_RESUME_TEST_MARKER", str(marker))

    db_path = tmp_path / "state.duckdb"
    first = run(FIXTURES / "resume_dag.py", db_path=db_path)
    assert not first.success
    assert first.statuses["root"] == "success"
    assert first.statuses["boom"] == "failed"
    assert first.statuses["downstream"] == "upstream_failed"

    marker.unlink()  # "fix the bug"
    second = run(FIXTURES / "resume_dag.py", db_path=db_path)
    assert second.success
    # No --resume flag, no special API -- re-running the exact same command
    # is "resume": root is untouched code with a cached result, so it's
    # reused; boom and downstream never previously succeeded, so they run.
    assert second.statuses["root"] == "skipped"
    assert second.statuses["boom"] == "success"
    assert second.statuses["downstream"] == "success"
    assert second.results["downstream"] == 20


def test_retries_recover_from_transient_failure(tmp_path):
    summary = run(FIXTURES / "retry_dag.py", db_path=tmp_path / "state.duckdb")

    assert summary.success
    assert summary.statuses["flaky"] == "success"
    assert summary.results["flaky"] == "ok"


def test_fanout_dag_runs_all_partitions(tmp_path):
    summary = run(FIXTURES / "fanout_dag.py", db_path=tmp_path / "state.duckdb")

    assert summary.success
    assert summary.results["partition_0"] == [0, 3, 6]
    assert summary.results["partition_1"] == [1, 4, 7]
    assert summary.results["partition_2"] == [2, 5, 8]
    assert summary.results["combine"] == "done"


def test_arrow_cache_backend_skips_on_second_run(tmp_path):
    import polars as pl
    import pyarrow as pa

    db_path = tmp_path / "state.duckdb"
    first = run(FIXTURES / "arrow_cache_dag.py", db_path=db_path)
    assert first.statuses["stage"] == "success"
    assert isinstance(first.results["stage"], pl.DataFrame)  # the task's own return value

    second = run(FIXTURES / "arrow_cache_dag.py", db_path=db_path)
    assert second.statuses["stage"] == "skipped"
    # A cache hit under the arrow backend hands back a plain pyarrow.Table
    # (DESIGN.md sec 6.2) -- not the original Polars DataFrame type.
    assert isinstance(second.results["stage"], pa.Table)
    assert second.results["stage"].to_pydict() == {"a": [1, 2, 3]}


def test_uncacheable_output_warns_but_does_not_fail_the_run(tmp_path):
    summary = run(FIXTURES / "unpicklable_cache_dag.py", db_path=tmp_path / "state.duckdb")

    assert summary.success
    assert summary.statuses["opens_a_live_handle"] == "success"


def test_state_file_is_queryable_directly_as_duckdb(tmp_path):
    import duckdb

    db_path = tmp_path / "state.duckdb"
    run(FIXTURES / "toy_dag.py", db_path=db_path)

    con = duckdb.connect(str(db_path), read_only=True)
    rows = con.execute("SELECT task_name, status FROM task_runs ORDER BY task_name").fetchall()
    con.close()

    assert rows == [
        ("extract", "success"),
        ("load", "success"),
        ("transform_a", "success"),
        ("transform_b", "success"),
    ]


def test_run_against_a_pipeline_with_tasks_split_across_sibling_files(tmp_path):
    summary = run(FIXTURES / "multi_module_pkg" / "pipeline.py", db_path=tmp_path / "state.duckdb")
    assert summary.success
    assert summary.results == {"extract": [1, 2, 3], "transform": [10, 20, 30]}


def test_bounded_concurrency_does_not_starve_an_independent_task_behind_a_blocked_dependent(
    tmp_path, monkeypatch
):
    """A concurrency slot must be earned by being ready to run, not by
    merely existing in the DAG. Before the fix, `combine` (which depends
    on `slow_root`) could win a slot immediately -- thanks to the
    alphabetical topological tie-break putting it right after its own
    dependency -- and then idle in that slot for slow_root's whole
    duration, leaving `zzz_independent` (unrelated, ready immediately)
    with no slot to run in until slow_root finished. Fixed, both start
    together regardless of `max_workers`."""
    timing_file = tmp_path / "timings.json"
    monkeypatch.setenv("DUCKPIPE_TEST_TIMING_FILE", str(timing_file))

    summary = run(FIXTURES / "starvation_dag.py", db_path=tmp_path / "state.duckdb", max_workers=2)

    assert summary.success
    times = json.loads(timing_file.read_text())
    assert abs(times["zzz_independent"] - times["slow_root"]) < 0.3, times


def test_unbounded_run_sizes_its_executor_to_the_dags_own_task_count(tmp_path, monkeypatch):
    """`max_workers=None` means unbounded -- but `loop.run_in_executor(None, ...)`
    would use Python's own default `ThreadPoolExecutor`
    (`min(32, cpu_count + 4)`) instead, silently capping a wide fan-out
    (DESIGN.md sec 4's own documented pattern) well below what the DAG's
    shape could actually support. Pins the fix: an explicit executor
    sized to the DAG's own task count, not the implicit default."""
    import duckpipe.scheduler as scheduler_module

    seen_sizes = []
    real_executor = scheduler_module.ThreadPoolExecutor

    class RecordingExecutor(real_executor):
        def __init__(self, *args, max_workers=None, **kwargs):
            seen_sizes.append(max_workers)
            super().__init__(*args, max_workers=max_workers, **kwargs)

    monkeypatch.setattr(scheduler_module, "ThreadPoolExecutor", RecordingExecutor)

    summary = run(FIXTURES / "fanout_dag.py", db_path=tmp_path / "state.duckdb", max_workers=None)

    assert summary.success
    assert seen_sizes == [5]  # source, partition_0/1/2, combine -- fanout_dag.py's own shape


def test_bounded_run_sizes_its_executor_to_max_workers(tmp_path, monkeypatch):
    import duckpipe.scheduler as scheduler_module

    seen_sizes = []
    real_executor = scheduler_module.ThreadPoolExecutor

    class RecordingExecutor(real_executor):
        def __init__(self, *args, max_workers=None, **kwargs):
            seen_sizes.append(max_workers)
            super().__init__(*args, max_workers=max_workers, **kwargs)

    monkeypatch.setattr(scheduler_module, "ThreadPoolExecutor", RecordingExecutor)

    summary = run(FIXTURES / "fanout_dag.py", db_path=tmp_path / "state.duckdb", max_workers=2)

    assert summary.success
    assert seen_sizes == [2]
