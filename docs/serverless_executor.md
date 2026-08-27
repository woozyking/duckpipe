# Serverless executor

Turned out to need no new mechanism — just checking that
[task-scoped execution](distributed_execution.md) genuinely isn't tied
to one platform or one coordination model.

`duckpipe.run(module, only=task, state_uri=...)` *is* the reference
executor — a plain Python call with no platform-specific glue.
[`../examples/07_serverless_executor`](../examples/07_serverless_executor/)
proves it by dispatching one DAG's two tasks into the same distributed
run through two genuinely different invocation shapes: a container
(`docker run`, argv-invoked — ECS/Cloud Run/a Kubernetes Job call the
same way) and a `handler(event, context)` function (the FaaS calling
convention Lambda/Modal/Cloud Functions actually use). Neither
`pipeline.py` nor DuckPipe itself knows or cares which one dispatched
it.

For one big task instead of many independent ones, see
[`remote_execution.md`](remote_execution.md) — "beefy node" mode needs
none of this machinery at all.
