from pathlib import Path

from typer.testing import CliRunner

from duckpipe.cli import app

FIXTURES = Path(__file__).parent / "fixtures"
runner = CliRunner()


def test_run_command_succeeds_and_reports_statuses(tmp_path):
    result = runner.invoke(
        app, ["run", str(FIXTURES / "toy_dag.py"), "--db", str(tmp_path / "state.duckdb")]
    )
    assert result.exit_code == 0
    assert "success" in result.stdout


def test_run_command_exits_nonzero_on_task_failure(tmp_path):
    result = runner.invoke(
        app, ["run", str(FIXTURES / "failing_dag.py"), "--db", str(tmp_path / "state.duckdb")]
    )
    assert result.exit_code == 1


def test_cyclic_dag_reports_a_friendly_one_line_error_not_a_traceback():
    result = runner.invoke(app, ["show", str(FIXTURES / "cyclic_dag.py")])
    assert result.exit_code == 1
    assert "Traceback" not in result.stdout
    assert "cycle detected" in result.stdout


def test_duplicate_task_name_reports_a_friendly_error():
    result = runner.invoke(app, ["show", str(FIXTURES / "duplicate_name_dag.py")])
    assert result.exit_code == 1
    assert "Traceback" not in result.stdout
    assert "disambiguate" in result.stdout


def test_show_reports_next_run_preview(tmp_path):
    db_path = tmp_path / "state.duckdb"
    runner.invoke(app, ["run", str(FIXTURES / "toy_dag.py"), "--db", str(db_path)])

    result = runner.invoke(app, ["show", str(FIXTURES / "toy_dag.py"), "--db", str(db_path)])
    assert result.exit_code == 0
    assert "skip" in result.stdout


def test_stats_command_runs_against_a_state_file(tmp_path):
    db_path = tmp_path / "state.duckdb"
    runner.invoke(app, ["run", str(FIXTURES / "toy_dag.py"), "--db", str(db_path)])

    result = runner.invoke(app, ["stats", str(db_path)])
    assert result.exit_code == 0
    assert "per-task stats" in result.stdout


def test_show_json_lists_topological_order_for_a_coordinator(tmp_path):
    result = runner.invoke(app, ["show", str(FIXTURES / "toy_dag.py"), "--json"])
    assert result.exit_code == 0

    import json

    rows = json.loads(result.stdout)
    names = [r["task"] for r in rows]
    assert names.index("extract") < names.index("transform_a")
    assert names.index("transform_a") < names.index("load")


def test_run_only_executes_a_single_task(tmp_path):
    db_path = tmp_path / "state.duckdb"
    result = runner.invoke(
        app,
        ["run", str(FIXTURES / "toy_dag.py"), "--db", str(db_path), "--only", "extract"],
    )
    assert result.exit_code == 0
    assert "extract" in result.stdout
    assert "transform_a" not in result.stdout


def test_run_only_unknown_task_reports_a_friendly_error(tmp_path):
    result = runner.invoke(
        app,
        ["run", str(FIXTURES / "toy_dag.py"), "--db", str(tmp_path / "s.duckdb"), "--only", "nope"],
    )
    assert result.exit_code == 1
    assert "Traceback" not in result.stdout
    assert "no task named" in result.stdout


def test_compact_command_runs_against_a_state_uri(tmp_path):
    bucket = tmp_path / "bucket"
    state_uri = f"file://{bucket / 'duckpipe.db'}"
    runner.invoke(
        app,
        [
            "run",
            str(FIXTURES / "toy_dag.py"),
            "--db",
            str(tmp_path / "scratch.duckdb"),
            "--state-uri",
            state_uri,
            "--only",
            "extract",
        ],
    )

    result = runner.invoke(app, ["compact", state_uri])
    assert result.exit_code == 0
    assert "compacted" in result.stdout


def _ducklake_catalog(tmp_path):
    return f"ducklake:sqlite:{tmp_path / 'pipeline.ducklake.sqlite'}"


def test_run_and_stats_against_a_ducklake_backend(tmp_path):
    catalog = _ducklake_catalog(tmp_path)
    result = runner.invoke(app, ["run", str(FIXTURES / "toy_dag.py"), "--db", catalog])
    assert result.exit_code == 0
    assert "success" in result.stdout

    stats_result = runner.invoke(app, ["stats", catalog])
    assert stats_result.exit_code == 0
    assert "DuckLake-backed" in stats_result.stdout

    snapshots_result = runner.invoke(app, ["stats", catalog, "--snapshots"])
    assert snapshots_result.exit_code == 0
    assert "task load succeeded" in snapshots_result.stdout


def test_ducklake_db_combined_with_only_reports_a_friendly_error(tmp_path):
    result = runner.invoke(
        app,
        [
            "run",
            str(FIXTURES / "toy_dag.py"),
            "--db",
            _ducklake_catalog(tmp_path),
            "--only",
            "extract",
        ],
    )
    assert result.exit_code == 1
    assert "Traceback" not in result.stdout
    assert "only=" in result.stdout
