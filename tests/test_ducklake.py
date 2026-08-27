"""Phase 3b: the DuckLake-backed state store -- an opt-in observability
upgrade (time travel over run history, schema evolution), not a rework of
Phase 3a's distributed mechanism (DESIGN.md sec 8). These need network on
a cold extension cache (`INSTALL ducklake`/`sqlite`), same as
examples/05_distributed_with_ducklake.

The Postgres-catalog test near the end covers the same backend's "bonus"
opt-in one layer further -- a dedicated, long-running metadata database
instead of a local file, for teams that want several DuckPipe deployments
sharing one catalog. Needs a working `docker` daemon; skipped cleanly
without one.
"""

import subprocess
import time
import uuid
from pathlib import Path

import pytest

from duckpipe.scheduler import run
from duckpipe.state import StateStore, is_ducklake

FIXTURES = Path(__file__).parent / "fixtures"


def _catalog(tmp_path: Path) -> str:
    return f"ducklake:sqlite:{tmp_path / 'pipeline.ducklake.sqlite'}"


def _docker_available() -> bool:
    try:
        return subprocess.run(["docker", "info"], capture_output=True, timeout=10).returncode == 0
    except (OSError, subprocess.TimeoutExpired):
        return False


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


# -- Postgres-backed catalog (DESIGN.md sec 8, Phase 3b's "bonus"): the
# same backend, generic enough that it already worked against a live
# Postgres with zero code changes -- these just check that for real,
# rather than leaving it as a documentation-only claim.


@pytest.mark.skipif(not _docker_available(), reason="needs a working docker daemon")
def test_ducklake_backend_works_against_a_postgres_catalog(tmp_path):
    container = f"duckpipe-test-pg-{uuid.uuid4().hex[:8]}"
    subprocess.run(
        [
            "docker",
            "run",
            "-d",
            "--name",
            container,
            "-e",
            "POSTGRES_PASSWORD=duckpipe",
            "-P",
            "postgres:17-alpine",
        ],
        check=True,
        capture_output=True,
        text=True,
        timeout=60,
    )
    try:
        port = (
            subprocess.run(
                ["docker", "port", container, "5432/tcp"],
                check=True,
                capture_output=True,
                text=True,
                timeout=10,
            )
            .stdout.strip()
            .rsplit(":", 1)[-1]
        )
        for _ in range(30):
            if (
                subprocess.run(
                    ["docker", "exec", container, "pg_isready", "-U", "postgres"],
                    capture_output=True,
                    timeout=5,
                ).returncode
                == 0
            ):
                break
            time.sleep(1)
        else:
            pytest.fail("postgres container never became ready")

        catalog = (
            f"ducklake:postgres:dbname=postgres host=localhost port={port} "
            "user=postgres password=duckpipe"
        )
        data_path = str(tmp_path / "data")

        # No data_path derivable for a non-file catalog -- and the error
        # says so in a way that points at the fix, not just the limit.
        with pytest.raises(ValueError, match="Postgres/MySQL"):
            run(FIXTURES / "toy_dag.py", db_path=catalog)

        first = run(FIXTURES / "toy_dag.py", db_path=catalog, data_path=data_path)
        assert first.success
        assert first.results["load"] == [2, 4, 6, 101, 102, 103]

        second = run(FIXTURES / "toy_dag.py", db_path=catalog, data_path=data_path)
        assert all(status == "skipped" for status in second.statuses.values())

        with StateStore(catalog, read_only=True) as store:
            assert store.is_ducklake
            assert store.last_status("extract") is not None
            assert len(store.snapshots()) > 0
    finally:
        subprocess.run(["docker", "rm", "-f", container], capture_output=True, timeout=30)
