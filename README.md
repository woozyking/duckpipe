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
uv add "duckpipe[ducklake]"     # + pytz, for the DuckLake observability backend
uv add duckpipe-tuning          # optional, separate package -- see below
```

## The whole mental model

- **`@task`** decorates a plain Python function. Any signature, any
  return type — the core never inspects what a task returns. A
  side-effect-only task (send a Slack alert, kick off a training run,
  write a log line) is exactly as first-class as one that moves data:
  just return `None`. Add `cache=True` if you want it to fire only once
  per code/upstream change instead of on every re-run — the same
  fingerprint mechanism that skips a data task skips a side effect too,
  since neither ever depended on inspecting a return value. The one
  caveat is the same one any retry-based system has: keep the side
  effect idempotent if you set `retries>0`, since a retried attempt
  could otherwise repeat part of it.
- **Dependencies are inferred from default argument values**: writing
  `def b(x=a)` where `a` is another task tells DuckPipe `b` depends on
  `a`, and at run time `x` receives `a`'s actual result. Tasks with no
  data dependency but a real ordering requirement — the common case for
  side-effect tasks — use the `@task(depends_on=[...])` escape hatch.
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
duckpipe run pipeline.py [--db PATH] [--state-uri URI] [--force] [--max-workers N] [--only TASK]
duckpipe show pipeline.py [--db PATH] [--json]      # resolved DAG, last-run status, and what the *next* run would do
duckpipe stats duckpipe.db [--limit N] [--snapshots] # recent runs + per-task timing, or DuckLake time travel
duckpipe compact state_uri                          # fold distributed workers' pending state into one file
```

`--db` accepts a plain path or a `ducklake:...` catalog string — see
[DuckLake observability upgrade](#ducklake-observability-upgrade) below.

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

Realistic pipelines over real, bundled open data (NYC TLC taxi trips)
live in [`examples/`](examples/README.md): a daily batch ETL and a
fan-out-over-partitions pipeline, each shipped as an apples-to-apples
DuckDB/Polars pair (`duck.py`/`pl.py`) that stays lazy/streaming end to
end; one dedicated example that deliberately materializes mid-pipeline
and explains exactly why; a real multi-process distributed cluster run
using nothing but DuckPipe's own delta-merge mechanism, plus the same
coordination problem solved with DuckLake instead; and a DuckLake
observability example showing time travel over run history and
schema-evolution-with-no-migration concretely. Every non-distributed
example also runs unmodified against the full public dataset by setting
one environment variable; see
[`examples/data/README.md`](examples/data/README.md).

## Scaling to remote storage

`state_uri` syncs the `.duckdb` state file to/from S3/GCS/Azure/local
before and after a run (download-mutate-upload, since DuckDB's own file
format only supports read-only remote `ATTACH`). This is what makes
DuckPipe safe to run inside another orchestrator's ephemeral,
container-per-invocation workers — see ROADMAP.md §2, §9.

Two overlapping invocations against the same `state_uri` hold an
advisory lock for the whole download-run-upload sequence (via each
object store's native conditional-write primitive — no extra server or
database needed), so a race raises `StateLockedError` instead of
silently losing an update. Pass `lock=False`/`--no-lock` to opt out.

```bash
duckpipe run pipeline.py --state-uri s3://my-bucket/pipelines/daily/duckpipe.db
```

Embedding a pipeline inside Airflow/Dagster/Prefect, or triggering it
from cron/CI/Lambda/a webhook, follows the exact same "just call
`duckpipe.run(...)`" shape — see [`docs/triggers.md`](docs/triggers.md)
and [`docs/interop.md`](docs/interop.md) for working recipes.

## Distributed execution

`only=<task>` (`--only` on the CLI) runs exactly one task instead of the
whole DAG — the same command every trigger already calls, just narrower
in scope. Against a `state_uri`, a scoped run never takes the whole-file
lock above: it writes only its own new rows to a uniquely-keyed delta
file instead of re-uploading the whole state file, so many workers can
each run `--only` concurrently — on different tasks, or even the same
task redundantly — with no contention at all.

```bash
duckpipe run pipeline.py --only extract --state-uri s3://my-bucket/pipelines/daily/duckpipe.db
```

Something else decides which worker runs which task and in what order —
`duckpipe show --json` is the discovery primitive a coordinator needs
(topological order + which tasks would skip). `duckpipe compact
state_uri` folds workers' pending deltas into the canonical file — not
needed for correctness (every invocation already absorbs what's pending
itself), just for keeping `.pending/` from growing forever in a purely
distributed workflow that never does a whole run. See
[`examples/04_distributed_cluster`](examples/04_distributed_cluster/)
for a real multi-process cluster run, and
[`examples/05_distributed_with_ducklake`](examples/05_distributed_with_ducklake/)
for the DuckLake-backed alternative.

## DuckLake observability upgrade

`db_path="ducklake:sqlite:pipeline.ducklake.sqlite"` — the same argument
a plain file goes in, pointed at a different kind of string. Nothing
else changes: `duckpipe run`/`show`/`stats` work exactly as before. What
it buys is real snapshot history: every task's outcome becomes its own
DuckLake commit, tagged with a plain-English message (`task extract
succeeded`), so `task_runs AT (VERSION => n)` turns "what happened" into
an actually-queryable history instead of a present-tense table — and
`ALTER TABLE ... ADD COLUMN` needs no migration step.

```bash
duckpipe run pipeline.py --db "ducklake:sqlite:pipeline.ducklake.sqlite"
duckpipe stats "ducklake:sqlite:pipeline.ducklake.sqlite" --snapshots
```

This is an *observability* upgrade, not a coordination one — deliberately
unrelated to `state_uri`/`only=` above (both raise a clear error if
combined with a `ducklake:` `db_path` rather than doing something
ill-defined; see `ROADMAP.md` §8 for the three independently-verified
reasons why one doesn't subsume the other). See
[`examples/06_ducklake_observability`](examples/06_ducklake_observability/)
for time travel and schema evolution demonstrated concretely. Needs
`uv add "duckpipe[ducklake]"` (just `pytz`; the `ducklake`/`sqlite`
DuckDB extensions themselves install on first use, over the network).

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
naming question). Phases 0-2.5, Phase 3a (task-scoped distributed
execution), and Phase 3b (DuckLake observability upgrade) are implemented
and covered by the test suite, examples, and docs above; see
`ROADMAP.md` §11 for what's done and what's next (Phase 3c/3d: serverless
executor, beefy-node mode; Phase 4: WASM/browser, spike done but not yet
a committed deliverable).
