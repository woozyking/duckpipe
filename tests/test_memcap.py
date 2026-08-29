"""@task(memory_limit_mb=...) -- an isolated-subprocess RSS cap that
records "oom" as a first-class status instead of a crash. Generalizes a
pattern proven in production dogfooding (a real multi-engine benchmark
harness); tests below exercise the actual watchdog, not a mock."""

from pathlib import Path

import pytest

from duckpipe._memcap import MemoryLimitExceeded, run_capped
from duckpipe.scheduler import run

FIXTURES = Path(__file__).parent / "fixtures"


def test_run_capped_returns_the_real_value_when_under_the_limit():
    # A local closure, not a module-level function: cloudpickle can only
    # pickle a test-module function *by reference* (re-import by dotted
    # name), which fails in a fresh subprocess since pytest's own test
    # modules aren't necessarily on a plain sys.path -- a closure forces
    # pickling by value instead, which is what a real task's own module
    # (a proper, importable pipeline file) wouldn't need to rely on.
    def light():
        return 6 * 7

    assert run_capped("light", light, {}, limit_mb=256) == 42


def test_run_capped_raises_when_over_the_limit():
    def hungry():
        import time

        blob = bytearray(300 * 1024 * 1024)
        time.sleep(0.5)
        return len(blob)

    with pytest.raises(MemoryLimitExceeded) as exc_info:
        run_capped("hungry", hungry, {}, limit_mb=100)
    assert exc_info.value.task_name == "hungry"
    assert exc_info.value.limit_mb == 100
    assert exc_info.value.peak_mb > 100


def test_end_to_end_oom_is_recorded_not_crashed(tmp_path):
    summary = run(FIXTURES / "memcap_dag.py", db_path=tmp_path / "state.duckdb", only="hungry")

    assert not summary.success
    assert summary.statuses["hungry"] == "oom"
    assert "memory_limit_mb" in summary.errors["hungry"]


def test_oom_cascades_to_downstream_like_any_other_failure(tmp_path):
    summary = run(FIXTURES / "memcap_dag.py", db_path=tmp_path / "state.duckdb")

    assert summary.statuses["hungry"] == "oom"
    assert summary.statuses["downstream"] == "upstream_failed"
    assert summary.statuses["light"] == "success"  # a sibling, unaffected


def test_capped_task_under_limit_runs_and_caches_normally(tmp_path):
    db_path = tmp_path / "state.duckdb"
    first = run(FIXTURES / "memcap_dag.py", db_path=db_path, only="light")
    assert first.statuses["light"] == "success"
    assert first.results["light"] == 42

    second = run(FIXTURES / "memcap_dag.py", db_path=db_path, only="light")
    assert second.statuses["light"] == "skipped"
    assert second.results["light"] == 42


def test_capped_task_normal_error_is_failed_not_oom(tmp_path):
    summary = run(FIXTURES / "memcap_dag.py", db_path=tmp_path / "state.duckdb", only="raises")

    assert summary.statuses["raises"] == "failed"
    assert "ValueError" in summary.errors["raises"]


def test_oom_counts_toward_v_task_stats_failed_count(tmp_path):
    """An oom'd task must not silently vanish from `duckpipe stats` --
    the same undercounting bug DESIGN.md sec 11 was careful to avoid for
    upstream_failed applies just as much to a new status value."""
    from duckpipe.state import StateStore

    db_path = tmp_path / "state.duckdb"
    run(FIXTURES / "memcap_dag.py", db_path=db_path, only="hungry")

    with StateStore(db_path, read_only=True) as store:
        row = store.con.execute(
            "SELECT failed_count FROM v_task_stats WHERE task_name = 'hungry'"
        ).fetchone()
    assert row == (1,)
