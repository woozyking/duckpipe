# 07 — serverless executor, two platforms, one run

The reference serverless executor (DESIGN.md sec 8): not a new
mechanism — `duckpipe.run(module, only=task, state_uri=...)` already
*is* the whole "serverless executor," unchanged. What was missing was
checking, not just claiming, that it's genuinely not tied to one
platform's invocation model. This example dispatches `pipeline.py`'s two
tasks into *one* distributed run, each through a different shape:

- `extract` runs inside a container (`Dockerfile` — argv-invoked, the
  way a bare `docker run`, ECS, Cloud Run, or a Kubernetes Job calls
  one).
- `summarize` runs through `handler.py` (`handler(event, context)` — the
  calling convention AWS Lambda, Modal, and most FaaS platforms actually
  use: an event dict in, a result dict out, no process/argv involved at
  all).

Both are the same one-line call underneath. Nothing in `pipeline.py` (or
in DuckPipe itself) knows or cares which shape dispatched it.

```bash
docker build -f examples/07_serverless_executor/Dockerfile -t duckpipe-worker .
uv run python examples/07_serverless_executor/run_serverless_demo.py
```

What happens: the script dispatches `extract` via `docker run
duckpipe-worker --only extract --state-uri file:///data/duckpipe.db`,
then dispatches `summarize` via a direct `handler({"task": "summarize",
"state_uri": ..., "run_id": ...})` call — the exact shape a FaaS
platform's own runtime would call it with. Both write into the same
`state_uri`-backed bucket, coordinating with nothing but the delta-merge
mechanism from `../04_distributed_cluster`, then `duckpipe compact`
folds both workers' deltas into one file to report on.

**Making this a real deployment**, beyond this local proof:

- Swap `file:///data/duckpipe.db` for `s3://`/`gs://`/`az://` and the
  container shape is already a genuine ECS/Cloud Run/Kubernetes-Job
  deployment — the image doesn't change.
- Point `handler.py` at real Lambda/Modal/Cloud Functions per that
  platform's own deploy step (a zip upload, a `modal deploy`, etc.) —
  the handler body doesn't change either; see
  [`../../docs/triggers.md`](../../docs/triggers.md#aws-lambda) for the
  Lambda packaging recipe DuckPipe already documents for whole-DAG runs.
- **Quack** (DuckDB's own client-server protocol) deliberately doesn't
  fit anywhere in this picture: it needs a persistent DuckDB *server*,
  the opposite of "stateless executor." It stays available for a
  *coordinator* to optionally run for one distributed run's lifetime
  (spun up, used, torn down — the same shape as this example's own
  `run_serverless_demo.py`, which is itself outside DuckPipe on purpose,
  per tenet #1), never as part of DuckPipe's own sync layer.

See [`../../docs/remote_execution.md`](../../docs/remote_execution.md)
for "beefy node" mode — the other extension of the same primitive, for
when the bottleneck is one big task rather than many independent ones.
