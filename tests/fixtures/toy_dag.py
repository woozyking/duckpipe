"""Phase 0 spike fixture: the toy 4-task diamond DAG referenced in ROADMAP.md
sec 11 ("Prove the fingerprinting + skip-if-unchanged model works
end-to-end for a toy 4-task DAG")."""

from duckpipe import task


@task(cache=True)
def extract():
    return [1, 2, 3]


@task(cache=True)
def transform_a(data=extract):
    return [x * 2 for x in data]


@task(cache=True)
def transform_b(data=extract):
    return [x + 100 for x in data]


@task(cache=True)
def load(a=transform_a, b=transform_b):
    return a + b
