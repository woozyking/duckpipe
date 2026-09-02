# Examples

Realistic pipelines over the bundled NYC TLC taxi sample data
(`data/README.md`), each demonstrating a different facet of DuckPipe.
Every pipeline is a plain `.py` file with no dependencies beyond what it
imports — read it top to bottom, run it, read its `duckpipe*.db` state
file afterwards.

Two of them ship **apples-to-apples pairs**: the same pipeline shape
solved with DuckDB (`duck.py`) and with Polars (`pl.py`), so you can
compare how the two engines express the same thing. Both stay
lazy/streaming end to end — there's nothing in either worth an eager
checkpoint. The third example is the deliberate exception: a pipeline
that materializes on purpose, and is explicit about exactly why.

| Example | Shape | What it shows |
|---|---|---|
| [`01_daily_batch_etl`](01_daily_batch_etl/) — [`duck.py`](01_daily_batch_etl/duck.py) / [`pl.py`](01_daily_batch_etl/pl.py) | `extract → clean → load` | Lazy end to end in both engines; `cache=True` only on the one task that actually materializes something |
| [`02_fanout_partitions`](02_fanout_partitions/) — [`duck.py`](02_fanout_partitions/duck.py) / [`pl.py`](02_fanout_partitions/pl.py) | `{peek boroughs} → process_<borough>×N → combine` | Fan-out via a plain Python loop generating uniquely-named tasks; DuckDB needs its own `cursor()` per task, Polars needs nothing extra at all |
| [`03_mid_pipeline_materialization`](03_mid_pipeline_materialization/pipeline.py) | `daily_revenue → rolling_revenue → report` | The one example that *does* break laziness on purpose — materializes exactly at the one task that touches the full source file, and says why in its docstring; also a DuckDB→Polars engine handoff with zero glue code |
| [`04_distributed_cluster`](04_distributed_cluster/) | `extract → {by_payment_type, by_hour} → combined_report` | The same DAG dispatched across real worker *processes* via `--only`, coordinated with nothing but delta files on shared storage — no DuckLake, no Quack |
| [`05_distributed_with_ducklake`](05_distributed_with_ducklake/) | same coordination problem as 04 | The "obvious next increment" from 04: workers commit straight into one shared DuckLake-backed table instead of writing delta files — with the honest tradeoff (SQLite-catalog retries vs. a Postgres catalog) verified, not assumed |
| [`06_ducklake_observability`](06_ducklake_observability/) | `extract → clean → daily_totals`, DuckLake-backed | Time travel over run history and no-migration schema evolution, from the same `--db` argument a plain file goes in |
| [`07_serverless_executor`](07_serverless_executor/) | `extract → summarize` | The same distributed run from 04, with its two tasks dispatched through two genuinely different invocation shapes — a container and a `handler(event, context)` function — proving the "not locked to one platform" claim instead of asserting it |
| [`08_browser_wasm`](08_browser_wasm/) | `profile → scan_sensitive_columns → triage_report` | "Is this file safe to send anywhere?" — checking an export for sensitive-looking columns without uploading it to find out, running DuckPipe's own unmodified source entirely inside a browser tab via Pyodide |
| [`09_nested_pipeline`](09_nested_pipeline/) | `{report_card, report_cash} → combine`, each nesting its own 3-task `extract → clean → aggregate` | A task's body running another whole pipeline via `duckpipe.run()` (confirmed safe) — and `to_mermaid`'s `subgraphs=` making that nesting visible instead of two opaque nodes |
| [`10_orchestrator_pools`](10_orchestrator_pools/) | `extract → {fast_a, fast_b, fast_c}`, `{fast_a, fast_b} → {publish_a, publish_b}` (rate-limited, shared pool) | `max_workers` is deliberately one global number — this bridges the "two tasks share a resource, forty others shouldn't wait on it" case into Airflow pools / Prefect tagged concurrency limits instead of growing a resource-group concept of DuckPipe's own |
| [`11_sustainability`](11_sustainability/) | `extract → clean → join_boroughs → aggregate → report`, run through an identical `run_pipeline()` in both arms | Quantifying "no standing infrastructure" in real kWh/year and $/year, using sourced energy-per-vCPU figures and a real self-hosted Prefect setup instead of a vibe — the engine is held constant on purpose, so the whole delta is the eliminated standing orchestrator, not an engine-speed claim |

Run any of the first three (each `duck.py`/`pl.py` pair uses its own
`--db` and output paths so the two variants never collide):

```bash
uv run duckpipe run examples/01_daily_batch_etl/duck.py --db examples/01_daily_batch_etl/duckpipe.duck.db
uv run duckpipe show examples/01_daily_batch_etl/duck.py --db examples/01_daily_batch_etl/duckpipe.duck.db
uv run duckpipe stats examples/01_daily_batch_etl/duckpipe.duck.db
```

Run one again immediately and compare the `status` column — that's
fingerprint-based incrementality (DESIGN.md tenet #6), not a special
flag you had to remember.

04, 05, and 07 are each their own coordinator script — see their
READMEs for the one command that runs them (`uv run python
run_cluster.py` / `ducklake_cluster.py` / `run_serverless_demo.py`). 05
needs network access on its first run to fetch the `ducklake`/`sqlite`
DuckDB extensions; 07 needs a working `docker` daemon and its image
built once first (`docker build -f
examples/07_serverless_executor/Dockerfile -t duckpipe-worker .`). 06 is
run the same way as 01-03 above, just with `--db
"ducklake:sqlite:...pipeline.ducklake.sqlite"` in place of a plain path
— see its own README. 08 is a static web page, not a CLI invocation —
`uv run python prepare_bundle.py` once, then serve the folder and open
it in a browser; see its own README for why (and its honest limits).

09 is `uv run duckpipe run examples/09_nested_pipeline/pipeline.py
--max-workers 1` (the flag matters here specifically — see its own
README); its own README also shows `show_nested_mermaid.py`, the
`to_mermaid(..., subgraphs=...)` demonstration `duckpipe show --mermaid`
alone can't give you, recursive to any depth.

10 is `uv run python examples/10_orchestrator_pools/run_with_pools.py` —
its own coordinator script, the same shape as 04's, plus a named
per-task concurrency pool layered on top; see its own README for the
direct Airflow/Prefect translation.

11 is `uv run python examples/11_sustainability/measure_and_quantify.py`
— quick against the bundled sample, or against a real full-year
`DUCKPIPE_EXAMPLE_DATA` for the real, cited kWh/year numbers its own
README reports.

See `data/README.md` for how to point any of these at the full public
dataset instead of the bundled sample, with zero code changes.
