# Changelog

All notable changes to this project are documented here. Format follows
[Keep a Changelog](https://keepachangelog.com/en/1.0.0/); the
versioning/compatibility promise itself is still an open decision
(`DESIGN.md` §12) until a 1.0.

## [Unreleased]

## [0.3.0] - 2026-08-29

### Added

- `@task(memory_limit_mb=N)` — an opt-in per-task physical memory
  ceiling: runs the task in an isolated subprocess under an RSS
  watchdog, recording `status="oom"` on breach instead of a crash or a
  silent OS OOM-kill. Needs the new `duckpipe[memcap]` extra
  (`cloudpickle` + `psutil`); the core dependency tree is untouched
  otherwise. Generalizes a pattern proven in production dogfooding
  (a real multi-engine benchmark harness) into DuckPipe's core.
- `to_mermaid(..., subgraphs=...)` — render a task as a real nested
  Mermaid subgraph containing another pipeline's own shape, for a task
  whose body runs one (nesting `duckpipe.run()` inside a task is safe,
  confirmed directly). DuckPipe can't discover this relationship on its
  own; the pipeline author states it explicitly.
- `examples/09_nested_pipeline` — a concrete, runnable demonstration of
  both of the above: two nested sub-pipeline calls rendered as real
  Mermaid subgraphs, plus two real bugs the example itself caught by
  actually running it (see its own README).
- `docs/agent_authored.md` — DuckPipe's position on agent-built
  pipelines: the agent writes you a plain, reviewable Python file,
  rather than operating an opaque one on your behalf through a
  platform-specific API.

### Fixed

- `v_task_stats`/`v_run_summary`'s `failed_count` now includes `oom`
  alongside `failed`/`upstream_failed` — without this, an oom'd task
  would silently disappear from `duckpipe stats` instead of counting
  as a failure.

## [0.2.0] - 2026-08-28

### Added

- `duckpipe.to_mermaid()` / `duckpipe.to_json()` — the DAG rendering
  `duckpipe show --mermaid`/`--json` already did, now public and
  importable directly (`duckpipe.dag`), not just reachable by shelling
  out to the CLI. Surfaced by dogfooding DuckPipe into a real external
  project: a notebook wanting to embed a pipeline diagram had no way to
  get that string without a subprocess call.

### Changed

- `cli.py`'s `show` command now calls these public functions instead of
  private duplicates — no behavior change, same output.

### Fixed

- `duckdb` was imported at module level in `state.py`, so merely
  `import duckpipe` — even just for `Task`/`build_dag`/`to_mermaid`,
  never touching a state file — pulled in DuckDB's compiled extension
  regardless. Deferred into `StateStore._open_local`/`_attach_ducklake`,
  the only two places that actually call `duckdb.connect()`; nothing
  else in the import chain needs it (type hints stay safe under
  `from __future__ import annotations` + `TYPE_CHECKING`). Measured
  directly, same dogfooding project: **-24.3MB → -5.5MB** RSS overhead
  from `import duckpipe` alone on top of an already-loaded engine (a
  ~77% cut) — confirmed against a real benchmark run, not just in
  isolation: every non-DuckDB engine's peak RSS dropped back to within
  noise of a DuckPipe-free baseline, with zero timing or correctness
  change (identical cross-engine verification, identical OOM outcomes).

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

[0.3.0]: https://github.com/woozyking/duckpipe/releases/tag/v0.3.0
[0.2.0]: https://github.com/woozyking/duckpipe/releases/tag/v0.2.0
[0.1.0]: https://github.com/woozyking/duckpipe/releases/tag/v0.1.0
