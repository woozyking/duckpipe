# 06 — DuckLake observability upgrade

The exact pipeline shape from
[`01_daily_batch_etl`](../01_daily_batch_etl/), pointed at a DuckLake
catalog instead of a plain `.duckdb` file — Phase 3b's opt-in
observability upgrade (ROADMAP.md sec 8). Compare `pipeline.py` here to
`01_daily_batch_etl/duck.py`: the tasks are unchanged, only `db_path`
is different.

```bash
uv run duckpipe run examples/06_ducklake_observability/pipeline.py \
    --db "ducklake:sqlite:examples/06_ducklake_observability/pipeline.ducklake.sqlite"

# run it again to see a "skipped" entry land in the same readable history
uv run duckpipe run examples/06_ducklake_observability/pipeline.py \
    --db "ducklake:sqlite:examples/06_ducklake_observability/pipeline.ducklake.sqlite"

uv run duckpipe stats "ducklake:sqlite:examples/06_ducklake_observability/pipeline.ducklake.sqlite" --snapshots

uv run python examples/06_ducklake_observability/explore_history.py
```

`explore_history.py` demonstrates the two things this backend is
actually for:

- **Time travel over run history.** Every task's success/skip/failure is
  its own snapshot, tagged with a plain-English commit message (`task
  extract succeeded`, `task daily_totals skipped (unchanged)`) — not
  just present-tense state, but a real, queryable history:
  `SELECT * FROM task_runs AT (VERSION => <snapshot_id>)`.
- **Schema evolution with no migration step.** A plain
  `ALTER TABLE task_runs ADD COLUMN IF NOT EXISTS host VARCHAR` against
  a live, already-populated table — existing rows just get `NULL`,
  nothing is rewritten.

This is deliberately just `duckpipe run`/`show`/`stats` — the same
commands, the same mental model, a different `--db` string. What it's
*not*: a rework of the distributed mechanism in
[`04_distributed_cluster`](../04_distributed_cluster/) — `--only` and
`--state-uri` both refuse to combine with a `ducklake:` `--db`, on
purpose (see `ROADMAP.md` sec 8 for why they're separate upgrades, not
one subsuming the other).
