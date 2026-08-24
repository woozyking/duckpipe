import json
import time
from pathlib import Path

import pytest

from duckpipe.remote import StateLockedError, locked, sync_down, sync_up
from duckpipe.scheduler import run

FIXTURES = Path(__file__).parent / "fixtures"


def test_sync_up_then_down_roundtrips_state_file(tmp_path):
    local_db = tmp_path / "local.duckdb"
    remote_dir = tmp_path / "remote_bucket"
    remote_dir.mkdir()
    state_uri = f"file://{remote_dir}/duckpipe.db"

    run(FIXTURES / "toy_dag.py", db_path=local_db)
    sync_up(state_uri, local_db)
    assert (remote_dir / "duckpipe.db").exists()

    other_local_db = tmp_path / "downloaded.duckdb"
    sync_down(state_uri, other_local_db)
    assert other_local_db.read_bytes() == local_db.read_bytes()


def test_run_with_state_uri_syncs_down_and_up(tmp_path):
    remote_dir = tmp_path / "remote_bucket"
    remote_dir.mkdir()
    state_uri = f"file://{remote_dir}/duckpipe.db"
    local_db = tmp_path / "scratch.duckdb"

    summary = run(FIXTURES / "toy_dag.py", db_path=local_db, state_uri=state_uri)

    assert summary.success
    assert (remote_dir / "duckpipe.db").exists()

    # A second "ephemeral worker" starts with no local state at all, but
    # syncing down from state_uri should let it see the fingerprints and
    # skip every task -- this is the whole point of sec 9's remote-sync
    # design for container-per-invocation workers.
    fresh_local_db = tmp_path / "fresh_scratch.duckdb"
    second = run(FIXTURES / "toy_dag.py", db_path=fresh_local_db, state_uri=state_uri)
    assert all(status == "skipped" for status in second.statuses.values())


def test_sync_down_is_a_noop_when_remote_does_not_exist_yet(tmp_path):
    state_uri = f"file://{tmp_path / 'missing_bucket' / 'duckpipe.db'}"
    local_db = tmp_path / "local.duckdb"

    sync_down(state_uri, local_db)  # should not raise
    assert not local_db.exists()


def test_locked_blocks_a_second_concurrent_acquire(tmp_path):
    state_uri = f"file://{tmp_path / 'duckpipe.db'}"

    with locked(state_uri):
        with pytest.raises(StateLockedError):
            with locked(state_uri):
                pass  # pragma: no cover

    # Released on exit -- a later, non-overlapping acquire succeeds.
    with locked(state_uri):
        pass


def test_locked_reclaims_a_stale_lock(tmp_path):
    import fsspec

    state_uri = f"file://{tmp_path / 'duckpipe.db'}"
    fs, lock_path = fsspec.core.url_to_fs(state_uri + ".lock")
    with fs.open(lock_path, "wb") as f:
        f.write(json.dumps({"holder": "dead:123", "acquired_at": time.time() - 10}).encode())

    with locked(state_uri, max_lock_age=1.0):
        pass  # the 10s-old lock is well past the 1s staleness threshold


def test_run_raises_state_locked_error_instead_of_racing(tmp_path):
    state_uri = f"file://{tmp_path / 'duckpipe.db'}"

    with locked(state_uri):
        with pytest.raises(StateLockedError):
            run(FIXTURES / "toy_dag.py", db_path=tmp_path / "scratch.duckdb", state_uri=state_uri)


def test_run_with_lock_false_ignores_an_existing_lock(tmp_path):
    state_uri = f"file://{tmp_path / 'duckpipe.db'}"

    with locked(state_uri):
        summary = run(
            FIXTURES / "toy_dag.py",
            db_path=tmp_path / "scratch.duckdb",
            state_uri=state_uri,
            lock=False,
        )
    assert summary.success
