from pathlib import Path

import pytest

from duckpipe.dag import CycleError, DuplicateTaskNameError, build_dag

FIXTURES = Path(__file__).parent / "fixtures"


def test_toy_diamond_dag_topological_order_respects_edges():
    dag = build_dag(FIXTURES / "toy_dag.py")
    order = [t.name for t in dag.topological_order()]

    assert order.index("extract") < order.index("transform_a")
    assert order.index("extract") < order.index("transform_b")
    assert order.index("transform_a") < order.index("load")
    assert order.index("transform_b") < order.index("load")
    assert set(order) == {"extract", "transform_a", "transform_b", "load"}


def test_roots_are_tasks_with_no_upstream():
    dag = build_dag(FIXTURES / "toy_dag.py")
    assert [t.name for t in dag.roots()] == ["extract"]


def test_cycle_raises_cycle_error():
    with pytest.raises(CycleError):
        build_dag(FIXTURES / "cyclic_dag.py")


def test_duplicate_task_name_raises():
    with pytest.raises(DuplicateTaskNameError):
        build_dag(FIXTURES / "duplicate_name_dag.py")


def test_fanout_loop_produces_uniquely_named_tasks_discovered_from_a_list():
    dag = build_dag(FIXTURES / "fanout_dag.py")
    names = set(dag.tasks)
    assert names == {"source", "partition_0", "partition_1", "partition_2", "combine"}

    order = [t.name for t in dag.topological_order()]
    for i in range(3):
        assert order.index("source") < order.index(f"partition_{i}")
        assert order.index(f"partition_{i}") < order.index("combine")
