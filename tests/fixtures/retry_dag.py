from duckpipe import task

attempts = {"count": 0}


@task(retries=2, retry_delay=0)
def flaky():
    attempts["count"] += 1
    if attempts["count"] < 2:
        raise RuntimeError("transient failure")
    return "ok"
