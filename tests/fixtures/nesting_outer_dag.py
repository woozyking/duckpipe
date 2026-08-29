"""A tiny "benchmark pipeline" shape for testing to_mermaid's nested
subgraphs: `suite_02` stands in for a task whose body runs another real
pipeline (see toy_dag.py) via a nested `duckpipe.run(...)`. Deliberately
reuses the name `extract` too, to prove the outer and nested diagrams'
node ids don't collide."""

from duckpipe import task


@task
def extract():
    return "outer extract -- unrelated to the nested suite's own"


@task(depends_on=[extract])
def suite_02():
    return "wrapper task; its own body would call duckpipe.run() on toy_dag.py"


@task
def validate(x=suite_02):
    return x
