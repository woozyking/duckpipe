# DuckLake observability upgrade

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
unrelated to `state_uri`/`only=` (see [`distributed_execution.md`](distributed_execution.md);
both raise a clear error if combined with a `ducklake:` `db_path` rather
than doing something ill-defined; see [`../DESIGN.md`](../DESIGN.md) §8 for
the three independently-verified reasons why one doesn't subsume the
other). See [`../examples/06_ducklake_observability`](../examples/06_ducklake_observability/)
for time travel and schema evolution demonstrated concretely. Needs
`uv add "duckpipe[ducklake]"` (just `pytz`; the `ducklake`/`sqlite`
DuckDB extensions themselves install on first use, over the network).

## Bonus: a shared Postgres catalog, for multiple teams/tenants

For teams that already run one: the same `db_path` string can
point at a dedicated, long-running metadata database instead of a local
file — `db_path="ducklake:postgres:dbname=... host=..."`, with
`data_path` passed explicitly (a shared network path or object-storage
URI every deployment can reach). Nothing else about `duckpipe.run(...)`
changes; it's the same opt-in, one layer further. What it buys beyond
the SQLite catalog above: several DuckPipe deployments — different
teams, different tenants, different machines — sharing one catalog with
genuine concurrent-write support. Verified directly, not assumed: 8
concurrent commits against a real Postgres catalog all succeeded with no
retry logic at all, where the same test against a SQLite catalog failed
3 of 8 outright (see
[`../examples/05_distributed_with_ducklake`](../examples/05_distributed_with_ducklake/)
for that comparison in full). Entirely optional — the SQLite catalog
above needs no such infrastructure and is the right default for a
single team's own history.
