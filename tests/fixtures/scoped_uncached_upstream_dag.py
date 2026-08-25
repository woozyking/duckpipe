"""A task with no cache=True can still "complete" (fingerprint recorded),
but a scoped (--only) downstream run can't get its *value* without one --
this fixture exercises that distinction directly."""

from duckpipe import task


@task
def root():
    return 1


@task
def leaf(x=root):
    return x + 1
