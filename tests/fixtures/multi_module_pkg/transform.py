from duckpipe import task

from .extract import extract  # relative import -- the thing this fixture exercises


@task(cache=True)
def transform(data=extract):
    return [x * 10 for x in data]
