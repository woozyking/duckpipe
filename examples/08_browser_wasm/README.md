# 08 — DuckPipe in your browser

Phase 4's first real deliverable (ROADMAP.md sec 8/11): DuckPipe's own,
completely unmodified source running a real DAG with real caching,
entirely inside a browser tab, via [Pyodide](https://pyodide.org). No
install, no server, no upload — whatever file you give it never leaves
the page.

```bash
uv run python prepare_bundle.py   # copies src/duckpipe/ + sample data in, once
python3 -m http.server 8000       # any static file server works
# open http://localhost:8000/
```

(`fetch()`-ing local files needs an http(s) origin — opening `index.html`
directly via `file://` will hit CORS restrictions in most browsers.
Any static host works for the real thing too: GitHub Pages, Netlify, a
plain `nginx` — this is a static page, nothing server-side to deploy.)

**What actually happens**, in order: load the Pyodide runtime, load the
real `duckdb` Python package (a real SQL engine, not a stub — it ships
in Pyodide's own package repository), fetch DuckPipe's own
`task.py`/`dag.py`/`fingerprint.py`/`state.py`/`scheduler.py` (copied
verbatim by `prepare_bundle.py`, not rewritten a single line) into the
sandbox's virtual filesystem, `import duckpipe`, then call
`duckpipe.run('/pipeline.py', db_path='/state/duckpipe.db')` — the exact
same call the CLI makes. `asyncio.run()` inside the scheduler works
unmodified because Pyodide uses WASM JSPI (stack switching) to let it
block on the browser's own event loop without deadlocking; JSPI ships in
Chrome 137+ stable today with no flag.

`pipeline.py` (open it, it's a completely normal DuckPipe pipeline) is
schema-agnostic on purpose — `profile → numeric_summary → report` works
over the bundled NYC taxi sample or over any CSV/Parquet you pick from
your own machine, since the whole point of a browser demo is "bring
your own file."

**The two things this is actually for, checked, not just claimed:**

- **Your data never leaves the tab.** Pick "your own file" and the
  bytes go straight from the file picker into Pyodide's virtual
  filesystem via `arrayBuffer()` — no `fetch`, no `XMLHttpRequest`, no
  network request at all. Verified directly (Playwright, real
  Chromium): a small CSV of made-up names/scores gets profiled
  correctly with zero network activity for the file itself.
- **The same fingerprint-based skip-if-unchanged story survives a real
  reload**, not just one page's JS lifetime. State persists via
  IndexedDB (`FS.mount(IDBFS, ..., '/state')`); reload the page — a
  brand new Pyodide instance, no shared JS state — and run again with
  the same file: every task reports `skipped`. Verified directly, not
  assumed: `tests/test_browser_wasm_example` (needs `uv run playwright
  install chromium` once) drives an actual headless browser through
  exactly this reload-and-rerun sequence.

**Honest limitations** (ROADMAP.md sec 8 is explicit about these, this
example doesn't paper over them): no `state_uri` remote sync and no
DuckLake backend here — Pyodide's DuckDB build has no runtime-loaded
extensions (`httpfs`/`ducklake` unavailable), and whether `fsspec` even
works against Pyodide's virtual filesystem/CORS-constrained fetch model
is genuinely unverified, tracked as the next spike, not claimed here.
Single-threaded (a Pyodide constraint on DuckDB's own internal
parallelism, not on DuckPipe's `asyncio`-based task concurrency — several
independent tasks still run concurrently the same way they do natively).
