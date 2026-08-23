# Examples

Three realistic pipelines over the bundled NYC TLC taxi sample data
(`data/README.md`), each demonstrating a different facet of DuckPipe.
Every pipeline is a plain `.py` file with no dependencies beyond what it
imports — read it top to bottom, run it, read `duckpipe.db` afterwards.

| Example | Shape | What it shows |
|---|---|---|
| [`01_daily_batch_etl`](01_daily_batch_etl/pipeline.py) | `extract → clean → load` | The default posture: lazy DuckDB relations end to end, `cache=True` only on the one task that actually materializes something |
| [`02_fanout_partitions`](02_fanout_partitions/pipeline.py) | `{peek} → process_<partition>×N → combine` | Fan-out via a plain Python loop generating uniquely-named tasks, each using its own `cursor()` off a shared connection |
| [`03_incremental_sql_chain`](03_incremental_sql_chain/pipeline.py) | `daily_revenue → rolling_revenue → report` | Fully eager + fully cached — an unchanged re-run skips the *entire* chain, not just the last step |

Run any of them:

```bash
uv run duckpipe run examples/01_daily_batch_etl/pipeline.py
uv run duckpipe show examples/01_daily_batch_etl/pipeline.py
uv run duckpipe stats examples/01_daily_batch_etl/duckpipe.db
```

Run one again immediately and compare the `status` column — that's
fingerprint-based incrementality (ROADMAP.md tenet #6), not a special
flag you had to remember.

See `data/README.md` for how to point any of these at the full public
dataset instead of the bundled sample, with zero code changes.
