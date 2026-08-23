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
    summary = run(FIXTURES / "failing_dag.py", db_path=tmp_path / "state.duckdb")

    assert not summary.success
    assert summary.statuses["ok_root"] == "success"
    assert summary.statuses["boom"] == "failed"
    assert summary.statuses["downstream"] == "failed"
    assert summary.statuses["sibling"] == "success"
    assert summary.results["sibling"] == 10


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
