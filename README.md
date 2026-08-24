# DuckPipe

A serverless-first, DuckDB-native pipeline orchestrator. No scheduler
daemon, no metadata Postgres, no broker — a run is a Python process that
starts, does work, records what it did to a `.duckdb` file, and exits.

```python
# pipeline.py
import duckdb
from duckpipe import task, run

@task
def extract():
    return duckdb.sql("SELECT * FROM read_parquet('trips.parquet')")

@task(cache=True)
def daily_totals(trips=extract):
    return trips.aggregate("date_trunc('day', ts) AS day, sum(fare) AS total").pl()

if __name__ == "__main__":
    run(__file__)
```

```bash
uv run duckpipe run pipeline.py
```

That's the whole surface area. `daily_totals(trips=extract)` is how you
declare a dependency — no `depends_on=[...]` boilerplate, no separate DAG
object, just a normal Python default argument. Run it again and
`daily_totals` reports `skipped`: nothing about its code or `extract`'s
changed, so there's nothing to redo.

## Why

Most teams reach for Airflow/Prefect/Dagster the moment they need "more
than one script that depends on another script," which means standing up
a scheduler, a metadata database, and usually a broker before running a
single real pipeline — fixed infrastructure tax whether your DAG moves a
thousand rows once a day or a billion rows every minute. Most pipeline
DAGs are a handful of dependent steps over data that fits on one
machine. DuckPipe is the orchestrator for *that* case: a library, not a
platform. See [`ROADMAP.md`](ROADMAP.md) for the full design rationale,
prior-art landscape check, and phased plan this implementation follows.

## Install

Requires Python ≥3.12; developed against 3.14.

```bash
uv add duckpipe                 # core: duckdb + typer + rich
uv add "duckpipe[remote]"       # + fsspec, for state_uri sync
uv add "duckpipe[s3]"           # + s3fs
uv add "duckpipe[arrow]"        # + pyarrow, for cache_backend="arrow"
uv add duckpipe-tuning          # optional, separate package -- see below
```

## The whole mental model

- **`@task`** decorates a plain Python function. Any signature, any
  return type — the core never inspects what a task returns.
- **Dependencies are inferred from default argument values**: writing
  `def b(x=a)` where `a` is another task tells DuckPipe `b` depends on
  `a`, and at run time `x` receives `a`'s actual result. Tasks with no
  data dependency but a real ordering requirement use the
  `@task(depends_on=[...])` escape hatch.
- **A pipeline is a Python module.** `duckpipe run pipeline.py` imports
  it once, discovers every `@task`-decorated function reachable from the
  module namespace, and runs the resulting DAG.
- **State is a `.duckdb` file** next to the pipeline (path configurable).
  `SELECT * FROM task_runs` in any DuckDB client tells you what happened
  — no separate UI.
- **Caching is `@task(cache=True)`.** DuckPipe fingerprints a task's
  source + config + upstream fingerprints (never its output data) and,
  on a cache hit, skips the task and hands the cached value downstream.
  `--force` re-runs everything regardless. Default cache storage is
  pickle; `cache_backend="arrow"` (needs `duckpipe[arrow]`) is a leaner
  alternative for tabular results — a DuckDB relation, pandas/Polars
  DataFrame, or pyarrow Table.
- **Resuming from a failure needs no special flag.** A failed task never
  gets a fingerprint or cached value, so re-running the exact same
  command only re-executes what failed (and anything downstream of it)
  — everything else with `cache=True` and unchanged code just skips, the
  same as any other unchanged re-run.
- **Retries are `@task(retries=N, retry_delay=seconds)`.**
- **Triggers are just "run the command."** Cron, CI, a webhook handler, a
  Lambda entrypoint, or a single step embedded inside Airflow/Prefect/
  Dagster all just call `duckpipe.run(...)` — DuckPipe has no scheduler
  daemon of its own.

No work pools, no deployments-as-a-separate-entity, no task-runner menu.
Full rationale for each of these choices — including the open questions
still being resolved — is in [`ROADMAP.md`](ROADMAP.md) §5, §11, §12.

## CLI

```bash
duckpipe run pipeline.py [--db PATH] [--state-uri URI] [--force] [--max-workers N]
duckpipe show pipeline.py [--db PATH]      # resolved DAG, last-run status, and what the *next* run would do
duckpipe stats duckpipe.db [--limit N]     # recent runs + per-task timing, from pre-built SQL views
```

A malformed pipeline (a dependency cycle, two tasks sharing a name) is
reported as one short line, not a framework traceback. `duckpipe show`
in particular doubles as a dry run: its "next run" column tells you
which tasks would skip vs. re-run before you spend the time actually
running it.

The state file's own views (`v_latest_task_status`, `v_run_summary`,
`v_task_stats`) are plain SQL and queryable from any DuckDB client, not
just through the CLI:

```sql
duckdb duckpipe.db -c "SELECT * FROM v_run_summary ORDER BY started_at DESC LIMIT 5"
```

## Examples

Three realistic pipelines over real, bundled open data (NYC TLC taxi
trips) live in [`examples/`](examples/README.md) — a daily batch ETL, a
fan-out-over-partitions pipeline, and a fully-cached incremental SQL
chain. Every one of them also runs unmodified against the full public
dataset by setting one environment variable; see
[`examples/data/README.md`](examples/data/README.md).

## Scaling to remote storage

`state_uri` syncs the `.duckdb` state file to/from S3/GCS/Azure/local
before and after a run (download-mutate-upload, since DuckDB's own file
format only supports read-only remote `ATTACH`). This is what makes
DuckPipe safe to run inside another orchestrator's ephemeral,
container-per-invocation workers — see ROADMAP.md §2, §9.

```bash
duckpipe run pipeline.py --state-uri s3://my-bucket/pipelines/daily/duckpipe.db
```

Embedding a pipeline inside Airflow/Dagster/Prefect, or triggering it
from cron/CI/Lambda/a webhook, follows the exact same "just call
`duckpipe.run(...)`" shape — see [`docs/triggers.md`](docs/triggers.md)
and [`docs/interop.md`](docs/interop.md) for working recipes.

## Tuning (optional, separate package)

`duckpipe-tuning` suggests DuckDB `threads`/`memory_limit` settings from
host specs (CPU count, RAM) — pure functions, no query execution, no
data inspection. It's a genuinely separate package in this repo's uv
workspace (`packages/duckpipe-tuning/`), not a submodule: `duckpipe`
itself never imports `psutil` or knows this package exists.

```python
import duckdb
from duckpipe_tuning import suggest_duckdb_settings

con = duckdb.connect()
settings = suggest_duckdb_settings(workload="join")
con.execute(f"SET threads = {settings['threads']}")
con.execute(f"SET memory_limit = '{settings['memory_limit']}'")
```

## Docs

- [`docs/why-duckpipe.md`](docs/why-duckpipe.md) — the pain points this
  design responds to, mapped to the actual code that answers each one.
- [`docs/triggers.md`](docs/triggers.md) — cron, GitHub Actions, Lambda,
  webhook recipes.
- [`docs/interop.md`](docs/interop.md) — embedding a pipeline inside
  Airflow/Dagster/Prefect.
- [`ROADMAP.md`](ROADMAP.md) — the full design rationale, prior-art
  landscape check, and phased plan this implementation follows.

## Development

This repo is a uv workspace: `duckpipe` at the root, `duckpipe-tuning`
under `packages/`.

```bash
uv sync --group dev
uv run pytest                                        # duckpipe
uv run --directory packages/duckpipe-tuning pytest   # duckpipe-tuning
uv run ruff check .
uv run python scripts/phase0_bench_fanout.py          # Phase 0 concurrency spike
```

CI (`.github/workflows/ci.yml`) runs all of the above on every push and
PR — also a live example of the GitHub Actions trigger recipe above.

## Status

Pre-1.0, working name (see [`ROADMAP.md`](ROADMAP.md) §0/§12 for the open
naming question). Phases 0-2 of the roadmap are implemented and covered
by the test suite, examples, and docs above; see `ROADMAP.md` §11 for
what's done and what's next (Phase 3: distributed/serverless execution).
