from duckpipe import task


@task(memory_limit_mb=256, cache=True)
def light():
    return 6 * 7


@task(memory_limit_mb=100)
def hungry():
    import time

    # Deliberately allocate well past the 100MB cap, then hold onto it
    # briefly -- gives the parent's watchdog (polling every 5ms) plenty
    # of chances to observe the spike before this process could exit on
    # its own, so the test isn't racing a coarse subprocess.poll() cycle.
    blob = bytearray(300 * 1024 * 1024)
    time.sleep(0.5)
    return len(blob)


@task
def downstream(x=hungry):
    return x


@task(memory_limit_mb=256)
def raises():
    raise ValueError("a normal task error, not an OOM")
