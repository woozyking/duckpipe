# Running in the browser

DuckPipe's own source runs, completely unmodified, inside a browser tab
via [Pyodide](https://pyodide.org) — a real DuckDB engine, a real DAG,
real fingerprint-based caching, no server involved at any point.
[`../examples/08_browser_wasm`](../examples/08_browser_wasm/) is a working
page built around a concrete niche, not a generic demo: **"is this file
safe to send anywhere?"** — checking an export for sensitive-looking
columns (emails, phone numbers, SSNs, credit cards, IPs) without
uploading it anywhere first, for anyone under an NDA, a legal hold, or a
compliance policy where "upload it to a checker" is itself the
violation. Pick "your own file" and the bytes go straight from your file
picker into the sandbox with zero network requests — your data never
leaves the tab — and reload the page after a run and it skips every
task, because state persisted across the reload via IndexedDB, not just
within one page's JS lifetime. Both verified directly with an actual
headless browser, not assumed. `asyncio.run()` inside the scheduler
works unmodified because Pyodide uses WASM JSPI (stack switching),
stable in Chrome 137+ today with no flag needed.

No remote sync or DuckLake backend in-browser yet — Pyodide's DuckDB
build has no runtime-loaded extensions, and whether `fsspec` works
against Pyodide's virtual filesystem is the next open spike, not
claimed here. See the example's own README for the full honest list,
including how conservative the sensitive-column checks are.
