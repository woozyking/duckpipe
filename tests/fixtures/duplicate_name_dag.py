from duckpipe import task


@task(name="dup")
def one():
    return 1


@task(name="dup")
def two():
    return 2
