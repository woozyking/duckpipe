"""Copies DuckPipe's real, unmodified source into this example's own
directory so `index.html` can `fetch()` it into Pyodide's virtual
filesystem and `import duckpipe` for real (ROADMAP.md sec 8, Phase 4) --
the same role `Dockerfile`'s `COPY src/ src/` plays in
`../07_serverless_executor`, just for a browser instead of a container.
Nothing here rewrites a single line; this is packaging, not porting.

Also copies the bundled sample dataset under a fixed name so the demo
pipeline doesn't need to know the original filename.

Run this once before serving the example (see README.md); re-run it any
time `src/duckpipe/` changes to keep the copy in sync -- both generated
directories are gitignored, not committed.
"""

import json
import shutil
from pathlib import Path

HERE = Path(__file__).parent
SRC = HERE.parent.parent / "src" / "duckpipe"
SAMPLE_DATA = HERE.parent / "data" / "nyc_taxi_sample.parquet"


def main() -> None:
    dest_pkg = HERE / "duckpipe_src" / "duckpipe"
    shutil.rmtree(dest_pkg.parent, ignore_errors=True)
    dest_pkg.mkdir(parents=True)
    py_files = sorted(SRC.glob("*.py"))
    for f in py_files:
        shutil.copyfile(f, dest_pkg / f.name)
    # index.html fetches this list rather than hardcoding filenames, so it
    # never drifts from whatever src/duckpipe/ actually contains.
    (dest_pkg.parent / "manifest.json").write_text(json.dumps([f.name for f in py_files]))
    print(f"copied {len(py_files)} files from {SRC} -> {dest_pkg}")

    dest_data = HERE / "sample_data"
    dest_data.mkdir(exist_ok=True)
    shutil.copyfile(SAMPLE_DATA, dest_data / "input.parquet")
    print(f"copied sample data -> {dest_data / 'input.parquet'}")


if __name__ == "__main__":
    main()
