# Changelog

All notable changes to this project are documented here. Format follows
[Keep a Changelog](https://keepachangelog.com/en/1.0.0/); versioning
intent is described in [`DESIGN.md`](DESIGN.md) §0 until a 1.0 promise
is made explicit.

## [Unreleased]

## [0.1.0] - 2026-08-27

First tagged release. Pre-1.0 — see [README.md](README.md) §Status for
what's explicitly *not* built yet.

### Added

- Core scheduler: `@task`, dependencies inferred from default-argument
  values, fingerprint-based caching (`cache=True`), retries, `--force`.
- CLI: `duckpipe run|show|stats|compact`, including a Mermaid DAG
  export (`show --mermaid`) colored by each task's last-run status.
- State as a plain `.duckdb` file with queryable views
  (`v_run_summary`, `v_task_stats`, `v_latest_task_status`) — no
  separate UI.
- Optional `fsspec`-backed remote state sync (`state_uri`) with an
  advisory lock, for S3/GCS/Azure/local.
- Task-scoped distributed execution (`only=`/`--only`) with
  delta-merge state, for many workers sharing one DAG without lock
  contention.
- DuckLake observability upgrade (`db_path="ducklake:..."`) — snapshot
  time travel and schema evolution, backed by a local SQLite or shared
  Postgres/MySQL catalog.
- Serverless-executor and beefy-node execution patterns, verified
  against a container and a FaaS-style `handler(event, context)`.
- Browser execution via Pyodide — DuckPipe's own source, unmodified,
  running entirely client-side with IndexedDB-persisted state.
- `duckpipe-tuning`: a separate, optional package suggesting DuckDB
  `threads`/`memory_limit` settings from host specs.
- Eight example pipelines (`examples/`) over real bundled NYC TLC taxi
  data, one per facet of the above.
- Docs site on GitHub Pages, including a live in-browser playground.

[0.1.0]: https://github.com/woozyking/duckpipe/releases/tag/v0.1.0
