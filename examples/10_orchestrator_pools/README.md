# 10 — bridging `max_workers` into a host orchestrator's own named concurrency control

DuckPipe's own concurrency knob, `max_workers`, is deliberately global
and single: one number for the whole pipeline (`DESIGN.md` §12,
"Concurrency default"). That's the right default — most pipelines don't
need more — but it has a real limit worth naming honestly: a pipeline
with 40 ordinary tasks and 2 that write to the same rate-limited
external API has no correct answer inside DuckPipe alone. `max_workers=1`
serializes all 42; leaving it unbounded lets the 2 race the API.

DuckPipe's own core stays exactly as simple as it is today — no new
`@task(pool=...)` parameter, no resource-group concept. Instead, this
example bridges the need to where it's already solved well: Airflow's
**pools** and Prefect's **tagged concurrency limits** both let you name
a resource and assign specific tasks to it, independent of everything
else in the pipeline. `pipeline.py`'s `POOLS`/`POOL_CAPACITY` are two
plain dicts — not a DuckPipe concept at all — that say exactly what a
pool assignment would need to say to either tool.

```
    extract ─┬─► fast_a ─► publish_a ─┐  } pool: external_api
             ├─► fast_b ─► publish_b ─┘  } (capacity 1)
             └─► fast_c
```

## Run it

```bash
uv run python examples/10_orchestrator_pools/run_with_pools.py
```

`run_with_pools.py` is a minimal, plain-`asyncio` stand-in for what
Airflow/Prefect's own schedulers already do — proving the technique
without either installed. Discovery is `duckpipe show --json` (the same
primitive [`examples/04_distributed_cluster`](../04_distributed_cluster/)
uses); dispatch is `duckpipe run --only <task> --state-uri ...`, the
same command any single-task worker runs for itself. The one thing this
script adds beyond example 04: instead of waiting for a whole dependency
"level" to finish before starting the next, each task dispatches the
moment its own dependencies are done, gated by a plain
`asyncio.Semaphore` per named pool for tasks that have one — exactly
mirroring an Airflow pool's slot count or a Prefect tagged limit — and
by nothing at all for tasks that don't.

The real, observed result of one run:

```
  extract      pool=(none)         0.00s -> 1.22s
  fast_c       pool=(none)         1.23s -> 2.55s
  fast_a       pool=(none)         1.22s -> 2.57s
  fast_b       pool=(none)         1.23s -> 2.58s
  publish_a    pool=external_api   2.57s -> 4.49s
  publish_b    pool=external_api   4.49s -> 6.63s

publish_a / publish_b overlapped: False  (pool capacity=1 -- must be False)
fast_a/fast_b/fast_c start spread: 0.01s -- small means they ran together, unaffected by the external_api pool
```

`publish_a`/`publish_b` never overlap — the pool's capacity of 1 is
genuinely enforced. `fast_a`/`fast_b`/`fast_c` start within 10ms of each
other regardless — the pool constrains only the two tasks assigned to
it, nothing else in the pipeline pays for it.

## The direct translation to a real host orchestrator

Same shape `docs/interop.md` already uses: the whole "integration" is a
few lines, not a maintained package. `POOLS`/`POOL_CAPACITY` map
directly onto each tool's own primitive — a pipeline author states the
mapping once, in plain data, and either scheduler enforces it using
machinery that already exists and is already battle-tested.

**Airflow** (a pool named `external_api` with 1 slot, created once via
the UI/CLI/`airflow pools set`, then referenced per task):

```python
from airflow.operators.python import PythonOperator
import duckpipe

def run_only(task_name):
    def _run(**context):
        summary = duckpipe.run(
            "/opt/pipelines/pipeline.py", only=task_name, state_uri="s3://.../duckpipe.db"
        )
        if not summary.success:
            raise RuntimeError(f"{task_name} failed: {summary.errors}")
    return _run

publish_a = PythonOperator(
    task_id="publish_a", python_callable=run_only("publish_a"), pool="external_api"
)
publish_b = PythonOperator(
    task_id="publish_b", python_callable=run_only("publish_b"), pool="external_api"
)
fast_a = PythonOperator(task_id="fast_a", python_callable=run_only("fast_a"))  # no pool = unbounded
```

**Prefect** (a named concurrency limit, created once via
`prefect gcl create external_api --limit 1`, referenced per task):

```python
from prefect import flow, task
from prefect.concurrency.sync import concurrency
import duckpipe

def run_only(task_name):
    summary = duckpipe.run(
        "/opt/pipelines/pipeline.py", only=task_name, state_uri="s3://.../duckpipe.db"
    )
    if not summary.success:
        raise RuntimeError(f"{task_name} failed: {summary.errors}")

@task
def publish_a_task():
    with concurrency("external_api", occupy=1):
        run_only("publish_a")

@task
def fast_a_task():
    run_only("fast_a")  # no concurrency() wrapper = unbounded
```

Both recipes make DuckPipe's own role exactly what `docs/interop.md`
already says it should be: an opaque, atomic `duckpipe run --only ...`
call the host orchestrator dispatches and waits on — success, failure,
duration, nothing more. Neither tool needs to know DuckPipe exists
beyond that one command; DuckPipe needs to know nothing about either.

## Why not just add this to DuckPipe's core

Considered directly and rejected. A `@task(pool=..., pool_capacity=...)`
parameter would be a genuinely new concept in the task-authoring surface
(`DESIGN.md` §5's own bar: does this let us delete something instead?),
and it would need DuckPipe to grow real scheduler-level state — named
semaphores tracked across the whole run, exactly the kind of
cross-cutting resource-management machinery tenet #3 already refuses to
duplicate once Airflow/Prefect already do it well. `only=` already gives
a coordinator everything it needs to make this decision itself, on its
own terms, with its own already-mature primitive — composition instead
of a second implementation of the same idea.
