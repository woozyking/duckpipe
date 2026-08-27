# Distributed execution

Two layers, each usable on its own: syncing state to remote storage so a
single ephemeral worker stays correct, and scoping a run down to one
task so many workers can safely contribute to the same DAG at once.

## Scaling to remote storage

`state_uri` syncs the `.duckdb` state file to/from S3/GCS/Azure/local
before and after a run (download-mutate-upload, since DuckDB's own file
format only supports read-only remote `ATTACH`). This is what makes
DuckPipe safe to run inside another orchestrator's ephemeral,
container-per-invocation workers — see [`../DESIGN.md`](../DESIGN.md) §2, §9.

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
`duckpipe.run(...)`" shape — see [`triggers.md`](triggers.md)
and [`interop.md`](interop.md) for working recipes.

## Task-scoped execution

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
[`../examples/04_distributed_cluster`](../examples/04_distributed_cluster/)
for a real multi-process cluster run, and
[`../examples/05_distributed_with_ducklake`](../examples/05_distributed_with_ducklake/)
for the DuckLake-backed alternative.

## Where to go from here

- [`ducklake.md`](ducklake.md) — an opt-in observability upgrade for the
  state file itself (time travel, schema evolution), separate from this
  coordination mechanism.
- [`serverless_executor.md`](serverless_executor.md) — this same
  `only=`/`state_uri` combination is already the reference serverless
  executor; nothing more to build.
- [`remote_execution.md`](remote_execution.md) — for one big task rather
  than many independent ones, a bigger remote machine needs none of this.
