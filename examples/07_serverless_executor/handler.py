"""The second invocation shape: a function-as-a-service handler
(AWS Lambda's `(event, context)` calling convention; Modal/Cloud
Functions/Azure Functions all look the same at this call site) --
genuinely different from the container in `Dockerfile`, which is a
process invoked with argv. No long-lived process, no CLI, no argv: an
event dict in, a result dict out, exactly the shape a FaaS platform
actually calls.

Deploying this behind real Lambda/Modal/etc. is a platform-specific
zip/container-image/CLI step with nothing to do with DuckPipe -- see
each platform's own docs. What belongs here, and what this example's
test actually exercises, is the one line that matters: the handler body
is `duckpipe.run(..., only=..., state_uri=...)`, unchanged from every
other invocation shape in this repo.
"""

from pathlib import Path
from typing import Any

from duckpipe import run

PIPELINE = Path(__file__).parent / "pipeline.py"


def handler(event: dict[str, Any], context: Any = None) -> dict[str, Any]:
    summary = run(
        PIPELINE,
        only=event["task"],
        state_uri=event["state_uri"],
        run_id=event.get("run_id"),
    )
    return {"success": summary.success, "statuses": summary.statuses, "errors": summary.errors}
