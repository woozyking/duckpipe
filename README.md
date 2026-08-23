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
  `--force` re-runs everything regardless.
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
duckpipe show pipeline.py [--db PATH]      # resolved DAG + last-run status
duckpipe stats duckpipe.db [--limit N]     # recent runs + per-task timing
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

## Development

```bash
uv sync --group dev
uv run pytest
uv run ruff check .
uv run python scripts/phase0_bench_fanout.py   # Phase 0 concurrency spike
```

## Status

Pre-1.0, working name (see [`ROADMAP.md`](ROADMAP.md) §0/§12 for the open
naming question). Phase 0 and Phase 1 of the roadmap are implemented and
covered by the test suite and examples above; see `ROADMAP.md` §11 for
what's done and what's next.
