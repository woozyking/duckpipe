from duckpipe import task


@task(cache=True)
def extract():
    return [1, 2, 3]
