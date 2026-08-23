# Example dataset

`nyc_taxi_sample.parquet` and `taxi_zone_lookup.csv` are trimmed from the
NYC Taxi & Limousine Commission's public **Yellow Taxi Trip Records** and
**Taxi Zone Lookup Table**, published as open data at
<https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page> and mirrored
at `https://d37ci6vzurychx.cloudfront.net/`.

- `nyc_taxi_sample.parquet` — the first week of January 2024, sampled down
  to ~12k rows and trimmed to the columns the examples actually use, so it
  loads instantly and stays small enough to commit.
- `taxi_zone_lookup.csv` — the full (tiny, ~260-row) zone-to-borough
  lookup table, unmodified.

Regenerate `nyc_taxi_sample.parquet` with:

```bash
uv run python scripts/fetch_sample_data.py
```

## Running the examples at scale

Every example pipeline reads `DATA` from the `DUCKPIPE_EXAMPLE_DATA`
environment variable, falling back to the bundled sample. DuckDB reads a
`https://` parquet URL directly (streaming, no download), so the exact
same pipeline code runs unmodified against the full public dataset:

```bash
DUCKPIPE_EXAMPLE_DATA="https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet" \
    uv run duckpipe run examples/01_daily_batch_etl/pipeline.py
```

That's ~3M rows for one month instead of ~12k, with zero code changes —
the same "local dev and production are the same code path" property
ROADMAP.md tenet #5 describes for distributed execution applies just as
well to scaling up the data itself. Swap in any other month's URL, or a
path to a bigger file you've downloaded yourself, the same way.
