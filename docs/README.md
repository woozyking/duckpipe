# Docs

Deeper chapters than the [top-level README](../README.md) has room for.
Each one is short and links back to the example code it describes.

**Using DuckPipe**

- [`why-duckpipe.md`](why-duckpipe.md) — the pain points this design
  responds to, mapped to the actual code that answers each one.
- [`triggers.md`](triggers.md) — cron, GitHub Actions, Lambda, webhook
  recipes.
- [`interop.md`](interop.md) — embedding a pipeline inside
  Airflow/Dagster/Prefect.

**Scaling out**

- [`distributed_execution.md`](distributed_execution.md) — syncing
  state to remote storage, and scoping a run to one task so many
  workers can share a DAG.
- [`ducklake.md`](ducklake.md) — the opt-in DuckLake observability
  upgrade (time travel, schema evolution), including a shared Postgres
  catalog for multiple teams.
- [`serverless_executor.md`](serverless_executor.md) — the same
  distributed-execution primitive, checked against two genuinely
  different invocation shapes.
- [`remote_execution.md`](remote_execution.md) — running unchanged on a
  bigger remote machine, for one big task instead of many small ones.
- [`browser.md`](browser.md) — DuckPipe's own source, unmodified, inside
  a browser tab via Pyodide.

**Design**

- [`../DESIGN.md`](../DESIGN.md) — the full design rationale, prior-art
  landscape check, and the reasoning behind every tenet.
