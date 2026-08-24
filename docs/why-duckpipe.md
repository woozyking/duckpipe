# Why DuckPipe

DuckPipe exists because the pain points below are real, specific, and
(per the landscape check in ROADMAP.md §4) unaddressed by the closest
prior art. Each row cites where the pain point came from and points at
the actual code in this repo that responds to it — not an aspiration.

| Pain point | Source | DuckPipe's answer | Where |
|---|---|---|---|
| Airflow's scheduler re-parses every DAG file on a fixed loop; degrades at scale | GitHub discussion #44727 | No persistent scheduler process at all — "trigger" is just invoking `duckpipe.run(...)`, which imports the module exactly once per invocation | [`src/duckpipe/scheduler.py`](../src/duckpipe/scheduler.py)'s `run()` |
| Airflow (FLYR postmortem): pre-allocated Celery workers waste spend; Kubernetes executor caused unexplained failures at high parallelism | FLYR Labs blog | No broker/worker-pool concept — concurrency is a plain `asyncio` + thread-pool executor, scaling out is "bigger single node" or a Phase 3 stateless executor, never a fleet to keep healthy | [`src/duckpipe/scheduler.py`](../src/duckpipe/scheduler.py) |
| Airflow: XCom size limits, awkward inter-task data passing | Airflow docs/discussions | Task-to-task handoff is a plain Python function call — no serialization, no size cap, in v1 | `Task.upstream_params()` result substitution in `_execute_dag` |
| Prefect: Flow→Deployment→Flow Run hierarchy confuses users (GitHub #10811, closed "not planned") | Prefect | No separate "deployment" entity — a pipeline module *is* the runnable unit; `duckpipe run pipeline.py` is the whole deploy story | [`src/duckpipe/cli.py`](../src/duckpipe/cli.py) |
| Prefect: work pools/workers/blocks/task-runner menu — "which one do I need" is a recurring question | Prefect docs, community threads | One concurrency model, period. No menu. | [`src/duckpipe/scheduler.py`](../src/duckpipe/scheduler.py) |
| Prefect: docs steer users to wrap pandas in `DaskTaskRunner` instead of the data engine's own parallelism | GitHub Discussion #3022 | The core never ships a competing parallelism/tuning primitive — DuckDB's own `threads`/`memory_limit` are yours to set; `duckpipe-tuning` is a genuinely separate opt-in package, not core-adjacent | [`packages/duckpipe-tuning/`](../packages/duckpipe-tuning/) |
| Prefect: 1→2→3 breaking rewrites even in typed codebases | Prefect migration guide, issue #15275 | A deliberately small, boring task-authoring surface (`@task`, default-value dependency inference, `depends_on`) — see the "whole mental model" in the README | [`README.md`](../README.md) |
| Dagster: assets-vs-ops split-brain — two overlapping graph paradigms | Discussion #10512 | One task/dependency model. No parallel paradigm to choose between. | [`src/duckpipe/task.py`](../src/duckpipe/task.py) |
| Dagster: "I/O managers aren't required" but a default one silently initializes anyway | Issue #32065 | No I/O-manager concept to be silently mandatory about — data never crosses the framework boundary unless you opt into `cache=True` | [`src/duckpipe/state.py`](../src/duckpipe/state.py) |
| No tool auto-tunes DuckDB resource settings to host specs | Our own DuckDB internals research | `duckpipe_tuning.suggest_duckdb_settings()` / `suggest_thread_count()` — pure host-spec functions, opt-in, zero core coupling | [`packages/duckpipe-tuning/src/duckpipe_tuning/__init__.py`](../packages/duckpipe-tuning/src/duckpipe_tuning/__init__.py) |
| No tool re-runs only what changed, by default, at the task level (dbt/SQLMesh do this for SQL models only) | SQLMesh docs, dbt `--state` docs | Fingerprint-based incrementality is default behavior for *any* task — not just SQL models, not behind a flag | [`src/duckpipe/fingerprint.py`](../src/duckpipe/fingerprint.py) |

## What running it actually looks like

Not a mockup — this is `examples/01_daily_batch_etl/pipeline.py` in full,
minus the module docstring:

```python
@task
def extract():
    return duckdb.sql(f"SELECT * FROM read_parquet('{DATA}')")

@task
def clean(trips=extract):
    return trips.filter("fare_amount > 0 AND trip_distance > 0 AND passenger_count > 0")

@task(cache=True)
def load(daily=clean):
    ...  # materializes into a warehouse table
    return row_count
```

```
$ uv run duckpipe run examples/01_daily_batch_etl/pipeline.py
      run 4804139c…  (examples/01_daily_batch_etl/duckpipe.db)
┏━━━━━━━━━┳━━━━━━━━━┓
┃ task    ┃ status  ┃
┡━━━━━━━━━╇━━━━━━━━━┩
│ extract │ success │
│ clean   │ success │
│ load    │ success │
└─────────┴─────────┘

$ uv run duckpipe run examples/01_daily_batch_etl/pipeline.py   # run it again
┏━━━━━━━━━┳━━━━━━━━━┓
┃ task    ┃ status  ┃
┡━━━━━━━━━╇━━━━━━━━━┩
│ extract │ success │
│ clean   │ success │
│ load    │ skipped │
└─────────┴─────────┘
```

No config file was written, no daemon was started, and `load`'s
incrementality is default behavior, not a flag anyone had to remember.
That's the whole pitch.
