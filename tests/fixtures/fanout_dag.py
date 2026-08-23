"""Fan-out via a plain Python loop generating uniquely-identified task
instances (ROADMAP.md sec 4's answer to large fan-out), instead of a
dynamic-mapping primitive baked into the core."""

from duckpipe import task


@task
def source():
    return list(range(9))


partitions = []
for i in range(3):

    @task(name=f"partition_{i}")
    def process(data=source, bucket=i):
        return [x for x in data if x % 3 == bucket]

    partitions.append(process)


@task(depends_on=partitions)
def combine():
    return "done"
