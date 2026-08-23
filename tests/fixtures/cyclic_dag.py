"""A real cycle can't arise from the default-value inference syntax alone
(an upstream Task object must exist before it can be used as a default),
so this fixture forces one via the `depends_on` escape hatch to exercise
CycleError."""

from duckpipe import task


@task
def a():
    return 1


@task
def b(x=a):
    return x


a.depends_on.append(b)  # manufactures a -> b -> a
