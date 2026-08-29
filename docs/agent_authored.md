# Agent-authored pipelines

A real, current thing as of this writing: [MotherDuck's Flights](https://motherduck.com/product/flights/)
connects an MCP-capable agent (Claude, Cursor, ChatGPT, or your own) to
a managed Python runtime — the agent gets tools to create, run,
schedule, inspect, version, and delete data pipelines through an API,
backed by DuckDB. That's a real, useful bet for a lot of teams, and it's
not what DuckPipe is.

DuckPipe is built for the other bet: **the agent writes you a pipeline,
it doesn't operate one for you.** The output of an agent session isn't
a pipeline living inside someone else's managed service, reachable only
through that service's own tools — it's a plain Python file, sitting in
your repo, that you can read top to bottom, `git diff`, run through your
own test suite, and understand without any platform-specific vocabulary
at all.

| What an agent-operated pipeline tool typically needs | DuckPipe's answer | Where |
|---|---|---|
| A platform-specific tool surface the agent has to learn (create/deploy/schedule/version primitives) | The agent already knows Python. `@task`, dependencies inferred from default-argument values, `duckpipe.run(...)` — the entire authoring surface, small enough to hold in context | README's "whole mental model" |
| Pipeline state and history live inside the vendor's own service, reachable through its API | A plain `.duckdb` file next to the pipeline, queryable with ordinary SQL from any client | [`src/duckpipe/state.py`](../src/duckpipe/state.py) |
| Trusting an opaque "logs" view to know what an agent actually built | `duckpipe show --mermaid`/`--json` — or `duckpipe.to_mermaid()`/`to_json()`, importable directly, no CLI subprocess needed — give a structural, inspectable answer to "what does this DAG actually do," not a log stream to read through | [`src/duckpipe/dag.py`](../src/duckpipe/dag.py) |
| A multi-level pipeline (a coordinator dispatching per-item sub-pipelines) is invisible from the outside — you see the coordinator's own log, not what it ran | `to_mermaid(..., subgraphs=...)` renders a task's own nested pipeline as a real subgraph, not a black box, whenever a task's body genuinely runs one (nesting `duckpipe.run()` inside a task is safe — [`DESIGN.md`](../DESIGN.md) §11) | [`src/duckpipe/dag.py`](../src/duckpipe/dag.py) |

## What this looks like in practice

Ask an agent to "add a task that dedupes trips by ID before loading,"
and the diff it produces is exactly the diff a person would write:

```python
@task(cache=True)
def dedupe(trips=clean):
    return trips.distinct()


@task(cache=True)
def load(daily=dedupe):  # was: daily=clean
    ...
```

Nothing about that requires the agent to call a platform API, register
a new pipeline version, or learn a deploy step — it's a function and an
edge, reviewed and merged the same way any other code change is. Ask it
to verify what it built, and `duckpipe show pipeline.py --mermaid` (or
`to_mermaid()` called directly, e.g. from inside the same session) gives
it — and you — a real, structural answer, not a claim to take on faith.

## The scaffolding already in this repo

None of the above needed new infrastructure — it's the same public API
this repo already ships, used by an agent instead of (or alongside) a
person. Two more pieces exist specifically so an agent can work with
this project well from the outside in:

- [`llms.txt`](https://woozyking.github.io/duckpipe/llms.txt) — a
  curated index for an LLM evaluating or citing DuckPipe, so it isn't
  crawling the whole site (or working from stale training data) to
  answer "how do I use this."
- [`AGENTS.md`](../AGENTS.md) — for a coding agent working *on* this
  repository itself (setup/test/lint commands, conventions), a
  different audience than everything above: contributing to DuckPipe,
  not authoring a pipeline with it.
