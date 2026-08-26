# Triggering a pipeline

DuckPipe has no scheduler daemon of its own (ROADMAP.md tenet #1, §5)
"triggers are just running the command." Every recipe below reduces to
one call: `duckpipe.run(...)` (Python) or `duckpipe run pipeline.py`
(shell). Pick whichever trigger you already operate.

## cron

```cron
# /etc/cron.d/daily-taxi-etl -- run at 02:00 every day
0 2 * * * pipelines cd /opt/pipelines && /opt/pipelines/.venv/bin/duckpipe run daily_etl.py >> /var/log/duckpipe/daily_etl.log 2>&1
```

Use absolute paths for both the working directory and the interpreter —
cron's environment is minimal and won't have `uv`/your shell's `PATH`.

## GitHub Actions

```yaml
# .github/workflows/daily-etl.yml
name: daily-etl
on:
  schedule:
    - cron: "0 2 * * *"
  workflow_dispatch: {}

jobs:
  run:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: astral-sh/setup-uv@v3
      - run: uv sync
      - run: uv run duckpipe run pipelines/daily_etl.py --state-uri s3://my-bucket/duckpipe.db
        env:
          AWS_ACCESS_KEY_ID: ${{ secrets.AWS_ACCESS_KEY_ID }}
          AWS_SECRET_ACCESS_KEY: ${{ secrets.AWS_SECRET_ACCESS_KEY }}
```

`--state-uri` matters here specifically because each workflow run is a
fresh, ephemeral container (ROADMAP.md §2/§9) — without it, every run
would start from zero fingerprints and never skip anything. See
[`../examples/data/README.md`](../examples/data/README.md) for the same
idea applied to data instead of state.

This repository's own [`.github/workflows/ci.yml`](../.github/workflows/ci.yml)
is a live, running example of the same pattern minus `--state-uri` (CI
runs want a clean slate every time, not incrementality).

## AWS Lambda

```python
# handler.py
import duckpipe


def handler(event, context):
    summary = duckpipe.run(
        "/var/task/pipeline.py",
        state_uri="s3://my-bucket/pipelines/daily/duckpipe.db",
    )
    return {"success": summary.success, "statuses": summary.statuses}
```

`state_uri` downloads the `.duckdb` state file before the run and
uploads it after (ROADMAP.md §2 tenet #1) — this is what makes
incrementality survive Lambda's container-per-invocation model, where
`/var/task` itself is thrown away between invocations. Package this with
`duckpipe[s3]` in your deployment artifact.

## A plain webhook handler

```python
# any WSGI/ASGI app
from fastapi import FastAPI
import duckpipe

app = FastAPI()


@app.post("/run")
def trigger():
    summary = duckpipe.run("pipeline.py")
    return {"success": summary.success}
```

That's the entire pattern for every trigger: something else decides
*when*; `duckpipe.run(...)` (or `duckpipe run`) decides *what happens
once triggered*, and never assumes it owns the host process — which is
also exactly what makes embedding DuckPipe inside another orchestrator
work; see [`interop.md`](interop.md).
