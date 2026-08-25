# Examples

Realistic pipelines over the bundled NYC TLC taxi sample data
(`data/README.md`), each demonstrating a different facet of DuckPipe.
Every pipeline is a plain `.py` file with no dependencies beyond what it
imports — read it top to bottom, run it, read its `duckpipe*.db` state
file afterwards.

Two of them ship **apples-to-apples pairs**: the same pipeline shape
solved with DuckDB (`duck.py`) and with Polars (`pl.py`), so you can
compare how the two engines express the same thing. Both stay
lazy/streaming end to end — there's nothing in either worth an eager
checkpoint. The third example is the deliberate exception: a pipeline
that materializes on purpose, and is explicit about exactly why.

| Example | Shape | What it shows |
|---|---|---|
| [`01_daily_batch_etl`](01_daily_batch_etl/) — [`duck.py`](01_daily_batch_etl/duck.py) / [`pl.py`](01_daily_batch_etl/pl.py) | `extract → clean → load` | Lazy end to end in both engines; `cache=True` only on the one task that actually materializes something |
| [`02_fanout_partitions`](02_fanout_partitions/) — [`duck.py`](02_fanout_partitions/duck.py) / [`pl.py`](02_fanout_partitions/pl.py) | `{peek boroughs} → process_<borough>×N → combine` | Fan-out via a plain Python loop generating uniquely-named tasks; DuckDB needs its own `cursor()` per task, Polars needs nothing extra at all |
| [`03_mid_pipeline_materialization`](03_mid_pipeline_materialization/pipeline.py) | `daily_revenue → rolling_revenue → report` | The one example that *does* break laziness on purpose — materializes exactly at the one task that touches the full source file, and says why in its docstring; also a DuckDB→Polars engine handoff with zero glue code |
| [`04_distributed_cluster`](04_distributed_cluster/) | `extract → {by_payment_type, by_hour} → combined_report` | Phase 3a: the same DAG dispatched across real worker *processes* via `--only`, coordinated with nothing but delta files on shared storage — no DuckLake, no Quack |
| [`05_distributed_with_ducklake`](05_distributed_with_ducklake/) | same coordination problem as 04 | The "obvious next increment" from 04: workers commit straight into one shared DuckLake-backed table instead of writing delta files — with the honest tradeoff (SQLite-catalog retries vs. a Postgres catalog) verified, not assumed |

Run any of the first three (each `duck.py`/`pl.py` pair uses its own
`--db` and output paths so the two variants never collide):

```bash
uv run duckpipe run examples/01_daily_batch_etl/duck.py --db examples/01_daily_batch_etl/duckpipe.duck.db
uv run duckpipe show examples/01_daily_batch_etl/duck.py --db examples/01_daily_batch_etl/duckpipe.duck.db
uv run duckpipe stats examples/01_daily_batch_etl/duckpipe.duck.db
```

Run one again immediately and compare the `status` column — that's
fingerprint-based incrementality (ROADMAP.md tenet #6), not a special
flag you had to remember.

04 and 05 are each their own coordinator script — see their READMEs for
the one command that runs them (`uv run python run_cluster.py` /
`uv run python ducklake_cluster.py`). 05 needs network access on its
first run to fetch the `ducklake`/`sqlite` DuckDB extensions.

See `data/README.md` for how to point any of these at the full public
dataset instead of the bundled sample, with zero code changes.
