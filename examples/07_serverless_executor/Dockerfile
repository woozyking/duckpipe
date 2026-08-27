# Build from the repo root, not this directory -- it needs `src/` and
# the bundled sample data:
#
#   docker build -f examples/07_serverless_executor/Dockerfile -t duckpipe-worker .
#
# The image is the entire "platform-specific" part of this story, and
# it's nothing: install duckpipe, copy the one pipeline file, done. The
# same image runs `--only <any task>` for any task in the DAG; which one
# is a `docker run` argument, not a rebuild.
FROM python:3.12-slim

WORKDIR /app
COPY pyproject.toml README.md LICENSE ./
COPY src/ src/
RUN pip install --no-cache-dir ".[remote]"

COPY examples/data/nyc_taxi_sample.parquet examples/data/nyc_taxi_sample.parquet
COPY examples/07_serverless_executor/pipeline.py examples/07_serverless_executor/pipeline.py

ENTRYPOINT ["duckpipe", "run", "examples/07_serverless_executor/pipeline.py"]
