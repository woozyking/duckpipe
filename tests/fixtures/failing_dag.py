from duckpipe import task


@task
def ok_root():
    return 1


@task
def boom(x=ok_root):
    raise ValueError("boom")


@task
def downstream(x=boom):
    return x


@task
def sibling(x=ok_root):
    return x * 10
