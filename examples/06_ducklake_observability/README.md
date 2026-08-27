# 06 — DuckLake observability upgrade

The exact pipeline shape from
[`01_daily_batch_etl`](../01_daily_batch_etl/), pointed at a DuckLake
catalog instead of a plain `.duckdb` file — DuckPipe's opt-in
observability upgrade (DESIGN.md sec 8). Compare `pipeline.py` here to
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
purpose (see `DESIGN.md` sec 8 for why they're separate upgrades, not
one subsuming the other).

## Bonus: a shared Postgres catalog, for multiple teams/tenants

Everything above uses `ducklake:sqlite:...`, a local file — the right
default for one team's own run history, no infrastructure required. The
exact same `--db`/`db_path` string can instead point at a dedicated,
long-running metadata database you already run, for teams that want
several DuckPipe deployments sharing one catalog:

```bash
docker run -d --name duckpipe-catalog -e POSTGRES_PASSWORD=duckpipe -p 5432:5432 postgres:17-alpine

uv run duckpipe run examples/06_ducklake_observability/pipeline.py \
    --db "ducklake:postgres:dbname=postgres host=localhost user=postgres password=duckpipe" \
    --data-path /srv/shared/ducklake_data   # or an s3://... URI every deployment can reach
```

Nothing else changes — same commands, same schema, same `--snapshots`.
`data_path` is the one required addition, since there's no local file to
derive a sibling data directory from the way there is for the SQLite
catalog above. What this buys: genuine concurrent-write support across
several deployments, not just one team's own history. Verified directly
against a real Postgres instance, not assumed from DuckLake's own docs:
8 concurrent commits with *no* retry logic all succeeded, where the
identical test against a SQLite catalog failed 3 of 8 outright — see
[`../05_distributed_with_ducklake`](../05_distributed_with_ducklake/)
for that comparison end to end. Entirely optional: nothing above needs
this, it's here for teams that already have a Postgres (or MySQL) and
want a shared catalog rather than one file per team.
