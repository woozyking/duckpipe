"""End-to-end checks for the in-repo examples (DESIGN.md Phase 1: "build
3-5 realistic example pipelines... have someone unfamiliar with the
project try them cold"). These run entirely against the bundled NYC TLC
taxi sample -- no network required -- so they're as fast and hermetic as
the rest of the suite (except 04/05, which need the `duckpipe` console
script and the `ducklake`/`sqlite` DuckDB extensions on PATH -- both are
available in this dev environment, but 05 does need network on a cold
extension cache; and 07, which needs a working `docker` daemon).
"""

import shutil
import subprocess
from pathlib import Path

import pytest

from duckpipe.scheduler import run

EXAMPLES = Path(__file__).parent.parent / "examples"


def _docker_available() -> bool:
    try:
        return subprocess.run(["docker", "info"], capture_output=True, timeout=10).returncode == 0
    except (OSError, subprocess.TimeoutExpired):
        return False


def _chromium_available() -> bool:
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        return False
    try:
        with sync_playwright() as p:
            p.chromium.launch().close()
        return True
    except Exception:
        return False


@pytest.mark.parametrize("engine", ["duck", "pl"])
def test_daily_batch_etl_example(tmp_path, engine):
    pipeline = EXAMPLES / "01_daily_batch_etl" / f"{engine}.py"
    warehouse_ext = "duckdb" if engine == "duck" else "parquet"
    warehouse = pipeline.parent / f"warehouse.{engine}.{warehouse_ext}"
    try:
        summary = run(pipeline, db_path=tmp_path / "state.duckdb")
        assert summary.success
        assert summary.results["load"] > 0
        assert warehouse.exists()
    finally:
        warehouse.unlink(missing_ok=True)


@pytest.mark.parametrize("engine", ["duck", "pl"])
def test_fanout_partitions_example(tmp_path, engine):
    pipeline = EXAMPLES / "02_fanout_partitions" / f"{engine}.py"
    output_dir = pipeline.parent / "output" / engine
    try:
        summary = run(pipeline, db_path=tmp_path / "state.duckdb")
        assert summary.success
        combine = summary.results["combine"]
        assert combine["partitions"] == len(combine["boroughs"])
        assert output_dir.exists()
        assert len(list(output_dir.glob("*.parquet"))) == combine["partitions"]
    finally:
        shutil.rmtree(output_dir.parent, ignore_errors=True)


def test_mid_pipeline_materialization_example(tmp_path):
    pipeline = EXAMPLES / "03_mid_pipeline_materialization" / "pipeline.py"
    report = pipeline.parent / "report.csv"
    try:
        db_path = tmp_path / "state.duckdb"
        first = run(pipeline, db_path=db_path)
        assert first.success
        assert report.exists()

        second = run(pipeline, db_path=db_path)
        assert second.success
        # Only the one full-file-scan task is worth caching (see the
        # module docstring); the cheap downstream tasks are deliberately
        # left uncached, so they re-run even though nothing changed.
        assert second.statuses["daily_revenue"] == "skipped"
        assert second.statuses["rolling_revenue"] == "success"
        assert second.statuses["report"] == "success"
    finally:
        report.unlink(missing_ok=True)


def test_distributed_cluster_example(tmp_path):
    example = EXAMPLES / "04_distributed_cluster"
    bucket = example / "cluster_bucket"
    try:
        result = subprocess.run(
            ["uv", "run", "python", "run_cluster.py"],
            cwd=example,
            capture_output=True,
            text=True,
            timeout=60,
        )
        assert result.returncode == 0, result.stdout + result.stderr
        assert "combined_report" in result.stdout
        assert (bucket / "duckpipe.db").exists()
    finally:
        shutil.rmtree(bucket, ignore_errors=True)
        for scratch in example.glob("_scratch_*.duckdb"):
            scratch.unlink()


def test_orchestrator_pools_example():
    example = EXAMPLES / "10_orchestrator_pools"
    bucket = example / "pools_bucket"
    try:
        result = subprocess.run(
            ["uv", "run", "python", "run_with_pools.py"],
            cwd=example,
            capture_output=True,
            text=True,
            timeout=60,
        )
        assert result.returncode == 0, result.stdout + result.stderr
        assert "publish_a / publish_b overlapped: False" in result.stdout
        assert (bucket / "duckpipe.db").exists()
    finally:
        shutil.rmtree(bucket, ignore_errors=True)
        for scratch in example.glob("_scratch_*.duckdb"):
            scratch.unlink()
        for scratch in example.glob("_scratch_*.duckdb.wal"):
            scratch.unlink()


def test_distributed_with_ducklake_example(tmp_path):
    example = EXAMPLES / "05_distributed_with_ducklake"
    try:
        result = subprocess.run(
            ["uv", "run", "python", "ducklake_cluster.py"],
            cwd=example,
            capture_output=True,
            text=True,
            timeout=60,
        )
        assert result.returncode == 0, result.stdout + result.stderr
        assert "combined_report" in result.stdout
        assert "success" in result.stdout
    finally:
        (example / "catalog.sqlite").unlink(missing_ok=True)
        shutil.rmtree(example / "data", ignore_errors=True)


@pytest.mark.skipif(not _docker_available(), reason="needs a working docker daemon")
def test_serverless_executor_example():
    example = EXAMPLES / "07_serverless_executor"
    bucket = example / "serverless_bucket"
    scratch = example / "duckpipe.db"
    root = EXAMPLES.parent
    try:
        build = subprocess.run(
            [
                "docker",
                "build",
                "-f",
                "examples/07_serverless_executor/Dockerfile",
                "-t",
                "duckpipe-worker",
                ".",
            ],
            cwd=root,
            capture_output=True,
            text=True,
            timeout=120,
        )
        assert build.returncode == 0, build.stdout + build.stderr

        result = subprocess.run(
            ["uv", "run", "python", "run_serverless_demo.py"],
            cwd=example,
            capture_output=True,
            text=True,
            timeout=60,
        )
        assert result.returncode == 0, result.stdout + result.stderr
        assert "container worker for 'extract'" in result.stdout
        assert "function worker for 'summarize'" in result.stdout
        assert "extract" in result.stdout and "summarize" in result.stdout
    finally:
        shutil.rmtree(bucket, ignore_errors=True)
        scratch.unlink(missing_ok=True)


def test_ducklake_observability_example():
    example = EXAMPLES / "06_ducklake_observability"
    catalog = example / "pipeline.ducklake.sqlite"
    data_dir = example / "pipeline.ducklake.sqlite.data"
    try:
        summary = run(example / "pipeline.py", db_path=f"ducklake:sqlite:{catalog}")
        assert summary.success

        result = subprocess.run(
            ["uv", "run", "python", "explore_history.py"],
            cwd=example,
            capture_output=True,
            text=True,
            timeout=60,
        )
        assert result.returncode == 0, result.stdout + result.stderr
        assert "task extract succeeded" in result.stdout
        assert "added `host` column" in result.stdout
    finally:
        catalog.unlink(missing_ok=True)
        shutil.rmtree(data_dir, ignore_errors=True)


def test_nested_pipeline_example(tmp_path):
    example = EXAMPLES / "09_nested_pipeline"
    outer_db = example / "duckpipe.db"
    card_db = example / "report_card.duckdb"
    cash_db = example / "report_cash.duckdb"
    # report_pipeline.py's own `summarize` task nests summary_pipeline.py,
    # keyed by DUCKPIPE_EXAMPLE_PAYMENT_TYPE (1/2 for card/cash, unset ->
    # "all" for a standalone run with no payment-type filter).
    summary_card_db = example / "summary_1.duckdb"
    summary_cash_db = example / "summary_2.duckdb"
    summary_all_db = example / "summary_all.duckdb"
    try:
        # --max-workers 1 is load-bearing, not a cheap default: report_card/
        # report_cash hand their payment type to their own nested call via
        # a process-wide env var, and running them concurrently races on
        # it (see this example's own README, "found the hard way").
        first = run(example / "pipeline.py", db_path=outer_db, max_workers=1)
        assert first.success
        assert first.results["combine"]["card"]["daily"]  # non-empty daily rows
        assert first.results["combine"]["cash"]["daily"]
        # The third, innermost nesting level's own result, surfaced all
        # the way up through report_pipeline.py's `summarize` task.
        assert first.results["combine"]["card"]["summary"]["total_trips"] > 0
        assert first.results["combine"]["cash"]["summary"]["total_trips"] > 0
        assert card_db.exists()
        assert cash_db.exists()
        assert summary_card_db.exists()
        assert summary_cash_db.exists()

        second = run(example / "pipeline.py", db_path=outer_db, max_workers=1)
        assert second.success
        assert second.statuses["report_card"] == "skipped"
        assert second.statuses["report_cash"] == "skipped"

        # The standalone sub-pipeline is independently runnable too, with
        # its own separate state file -- not the outer pipeline's.
        standalone_db = tmp_path / "report_standalone.duckdb"
        standalone = run(example / "report_pipeline.py", db_path=standalone_db)
        assert standalone.success
        assert standalone.results["aggregate"]
        assert standalone.results["summarize"]["total_trips"] > 0

        # show_nested_mermaid.py: the concrete, now-recursive `subgraphs=`
        # demonstration -- three real levels, not just one.
        result = subprocess.run(
            ["uv", "run", "python", "show_nested_mermaid.py"],
            cwd=example,
            capture_output=True,
            text=True,
            timeout=30,
        )
        assert result.returncode == 0, result.stdout + result.stderr
        assert 'subgraph t_report_card ["report_card"]' in result.stdout
        assert 'subgraph t_report_cash ["report_cash"]' in result.stdout
        assert 'subgraph t_report_card__t_summarize ["summarize"]' in result.stdout
        assert "t_report_card__t_summarize__t_totals" in result.stdout
        assert "class t_report_card skipped" in result.stdout
    finally:
        for db in (outer_db, card_db, cash_db, summary_card_db, summary_cash_db, summary_all_db):
            db.unlink(missing_ok=True)
            Path(str(db) + ".wal").unlink(missing_ok=True)


@pytest.mark.skipif(not _chromium_available(), reason="needs `uv run playwright install chromium`")
async def test_browser_wasm_example():
    import functools
    import http.server
    import threading

    from playwright.async_api import async_playwright

    example = EXAMPLES / "08_browser_wasm"
    subprocess.run(
        ["uv", "run", "python", "prepare_bundle.py"], cwd=example, check=True, timeout=30
    )

    handler = functools.partial(http.server.SimpleHTTPRequestHandler, directory=str(example))
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        async with async_playwright() as pw:
            browser = await pw.chromium.launch()
            page = await browser.new_page()
            await page.goto(f"http://127.0.0.1:{port}/index.html")

            # First run against the bundled sample: a real, cold pipeline run.
            await page.wait_for_function(
                "!document.getElementById('run-btn').disabled", timeout=60000
            )
            await page.click("#run-btn")
            await page.wait_for_function(
                "!document.getElementById('results').hidden", timeout=30000
            )
            statuses = await page.eval_on_selector("#status-table", "el => el.textContent")
            assert statuses.count("success") == 3, statuses
            verdict = await page.eval_on_selector("#verdict-banner", "el => el.textContent")
            assert "restricted" in verdict, verdict
            confirmed = await page.eval_on_selector("#confirmed-list", "el => el.textContent")
            assert "email" in confirmed and "ssn" in confirmed, confirmed

            # Reload -- a fresh Pyodide instance -- and run again: profile/
            # triage_report should skip, proving the state file genuinely
            # persisted across the reload via IndexedDB, not just within one
            # page's lifetime. scan_sensitive_columns is deliberately never
            # cached (pipeline.py's own docstring on it), so it re-runs every
            # time -- the contrast is the point, not a bug.
            await page.reload()
            await page.wait_for_function(
                "!document.getElementById('run-btn').disabled", timeout=60000
            )
            await page.click("#run-btn")
            await page.wait_for_function(
                "!document.getElementById('results').hidden", timeout=30000
            )
            statuses_after_reload = await page.eval_on_selector(
                "#status-table", "el => el.textContent"
            )
            assert statuses_after_reload.count("skipped") == 2, statuses_after_reload
            assert statuses_after_reload.count("success") == 1, statuses_after_reload

            await browser.close()
    finally:
        server.shutdown()
        thread.join(timeout=5)


def test_sustainability_example(tmp_path):
    # Exercises pipeline.py against the bundled sample only -- the real,
    # 12-month headline numbers this example's README reports come from
    # measure_and_quantify.py run by hand against a real DUCKPIPE_EXAMPLE_DATA
    # (network, multi-second), not something CI should do on every run.
    example = EXAMPLES / "11_sustainability"
    summary = run(example / "pipeline.py", db_path=tmp_path / "duckpipe.db")
    assert summary.success
    report = summary.results["report"]
    assert report["sources"] == 1
    assert report["total_trips"] > 0
    assert report["top_borough_by_revenue"] == "Manhattan"
