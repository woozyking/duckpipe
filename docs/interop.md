# Embedding DuckPipe inside another orchestrator

ROADMAP.md §9 states the design intent plainly: DuckPipe should always be
embeddable as a single task/step inside Airflow, Prefect, Dagster, or
anything else, **without any special integration code on DuckPipe's
side.** This is a free consequence of tenets #1 (no persistent daemon)
and #5 (local dev and production are the same code path) — a DuckPipe
pipeline is just `duckpipe.run(module)`, an importable Python call that
starts, does work, and exits, indistinguishable from any other function
those tools already know how to wrap.

There is deliberately no DuckPipe-side Airflow provider package, no
Dagster resource, no Prefect block. Each recipe below is the entire
integration — five lines, not a maintained package (§9's own stated
boundary).

## Airflow

```python
from airflow.operators.python import PythonOperator
import duckpipe


def run_duckpipe_pipeline(**context):
    summary = duckpipe.run("/opt/pipelines/daily_etl.py")
    if not summary.success:
        raise RuntimeError(f"duckpipe run failed: {summary.errors}")


task = PythonOperator(
    task_id="daily_etl_duckpipe",
    python_callable=run_duckpipe_pipeline,
)
```

Fifteen brittle, XCom-shuffling Airflow tasks can collapse into this one
— Airflow keeps doing what it's organizationally good at (cross-team
scheduling visibility, existing alerting, audit trail), while the
data-heavy inner loop gets DuckPipe's in-process data movement and
task-level fingerprint incrementality.

## Dagster

```python
from dagster import op
import duckpipe


@op
def daily_etl_duckpipe():
    summary = duckpipe.run("/opt/pipelines/daily_etl.py")
    if not summary.success:
        raise Exception(f"duckpipe run failed: {summary.errors}")
    return summary.results
```

## Prefect

```python
from prefect import task
import duckpipe


@task
def daily_etl_duckpipe():
    summary = duckpipe.run("/opt/pipelines/daily_etl.py")
    if not summary.success:
        raise RuntimeError(f"duckpipe run failed: {summary.errors}")
    return summary.results
```

## Local dev/test parity

The point of all three recipes above is that you iterate against the
DuckPipe pipeline directly — `uv run duckpipe run daily_etl.py`, seconds,
no scheduler or metadata-DB test harness — and only wrap the finished,
*unmodified* module in a one-line task when deploying. That directly
answers "DAGs are hard to test locally," one of the concrete pain points
this design responds to (see [`why-duckpipe.md`](why-duckpipe.md)).

## Surviving ephemeral, container-per-task workers

Airflow/Dagster/Prefect workers are frequently ephemeral containers —
each task attempt can land on a different filesystem. Point DuckPipe's
state file at durable storage with `state_uri` so incrementality
survives that:

```python
duckpipe.run(
    "/opt/pipelines/daily_etl.py",
    state_uri="s3://my-bucket/pipelines/daily_etl/duckpipe.db",
)
```

This downloads the `.duckdb` state file before the run and uploads it
after (requires the `duckpipe[s3]`/`[gcs]`/`[azure]` extra) — see
[`triggers.md`](triggers.md) for the same mechanism used from Lambda and
CI. An advisory lock is held for the duration by default, so if your host
orchestrator retries a still-running attempt or runs mapped partitions
concurrently against the *same* `state_uri`, the overlapping run raises
`StateLockedError` instead of racing (ROADMAP.md §12, open question #5).
Give each partition its own `state_uri` if they should genuinely run in
parallel.

## Where not to go

No DuckPipe-side awareness of the host orchestrator: no provider
package, no DuckPipe retries deferring to the host's retry semantics, no
fingerprints flowing through XCom. From the host's point of view,
`duckpipe.run(module)` stays an opaque, atomic unit — success, failure,
duration, nothing more. Nesting DuckPipe inside a heavier platform should
be a decision driven by real governance/observability needs the outer
platform provides, not something to do reflexively just because Airflow
happens to already be installed (ROADMAP.md §9).
