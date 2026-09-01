from pathlib import Path

import pytest

from duckpipe.dag import CycleError, DuplicateTaskNameError, build_dag, to_json, to_mermaid

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


def test_tasks_split_across_sibling_files_with_relative_imports():
    """A pipeline's tasks don't need to live in one file -- normal Python
    package composition (a relative import between task-definition files)
    just works, no DuckPipe-specific mechanism required (tenet #3)."""
    dag = build_dag(FIXTURES / "multi_module_pkg" / "pipeline.py")
    assert set(dag.tasks) == {"extract", "transform"}
    order = [t.name for t in dag.topological_order()]
    assert order == ["extract", "transform"]


def test_to_mermaid_is_importable_without_the_cli():
    """`duckpipe show --mermaid`'s exact rendering, but callable directly --
    a notebook or a custom dashboard shouldn't have to shell out to the CLI
    just to get this string."""
    dag = build_dag(FIXTURES / "toy_dag.py")
    order = dag.topological_order()
    diagram = to_mermaid(order)
    assert diagram.startswith("flowchart TD")
    assert "t_extract" in diagram
    assert "t_extract --> t_transform_a" in diagram


def test_to_mermaid_colors_by_last_status():
    dag = build_dag(FIXTURES / "toy_dag.py")
    order = dag.topological_order()
    diagram = to_mermaid(order, last_status={"extract": ("success", "2024-01-01")})
    assert "class t_extract success" in diagram
    assert "classDef success" in diagram


def test_to_json_omits_next_run_when_not_given():
    dag = build_dag(FIXTURES / "toy_dag.py")
    order = dag.topological_order()
    entries = to_json(order)
    assert all("next_run" not in e for e in entries)
    assert all(set(e) == {"task", "depends_on"} for e in entries)


def test_to_json_includes_next_run_when_given():
    dag = build_dag(FIXTURES / "toy_dag.py")
    order = dag.topological_order()
    entries = to_json(order, next_run={"extract": "run (no prior state)"})
    by_task = {e["task"]: e for e in entries}
    assert by_task["extract"]["next_run"] == "run (no prior state)"
    assert "next_run" not in by_task["transform_a"]


def test_to_mermaid_renders_a_nested_subgraph_with_namespaced_ids():
    """DuckPipe can't discover on its own that a task's body happens to
    run another pipeline -- `subgraphs` is how a pipeline author who
    *does* know that states it explicitly (DESIGN.md sec 5)."""
    outer_order = build_dag(FIXTURES / "nesting_outer_dag.py").topological_order()
    sub_order = build_dag(FIXTURES / "toy_dag.py").topological_order()

    diagram = to_mermaid(outer_order, subgraphs={"suite_02": (sub_order, None)})

    assert 'subgraph t_suite_02 ["suite_02"]' in diagram
    assert "    end" in diagram
    # The nested "extract" is namespaced under the wrapper task's own id --
    # never collides with the *outer* DAG's own, distinct "extract" task.
    assert "t_suite_02__t_extract" in diagram
    assert 't_extract["extract"]' in diagram  # the outer one, unprefixed
    assert diagram.count('["extract"]') == 2  # one outer, one (namespaced) inner
    # Edges inside the nested subgraph are namespaced consistently too.
    assert "t_suite_02__t_extract --> t_suite_02__t_transform_a" in diagram
    # The outer DAG's own edges are untouched by the nesting.
    assert "t_extract --> t_suite_02" in diagram
    assert "t_suite_02 --> t_validate" in diagram


def test_to_mermaid_nested_subgraph_colors_inner_tasks_by_their_own_status():
    outer_order = build_dag(FIXTURES / "nesting_outer_dag.py").topological_order()
    sub_order = build_dag(FIXTURES / "toy_dag.py").topological_order()
    sub_status = {"extract": ("success", "2024-01-01")}

    diagram = to_mermaid(
        outer_order,
        last_status={"suite_02": ("success", "2024-01-01")},
        subgraphs={"suite_02": (sub_order, sub_status)},
    )

    # The inner task is colored by the *sub-pipeline's own* history...
    assert "class t_suite_02__t_extract success" in diagram
    # ...and the wrapper task itself still gets its own outer status too --
    # both are meaningful, neither replaces the other.
    assert "class t_suite_02 success" in diagram
    # classDef is emitted exactly once even though both levels used it.
    assert diagram.count("classDef success") == 1


def test_to_mermaid_supports_recursive_subgraphs_to_any_depth():
    """A subgraph's own tuple takes an optional third element: its own
    `subgraphs` dict, the same shape one level deeper. Recursion isn't a
    separate mechanism -- it's this same argument, nested."""
    outer_order = build_dag(FIXTURES / "nesting_outer_dag.py").topological_order()
    middle_order = build_dag(FIXTURES / "toy_dag.py").topological_order()
    innermost_order = build_dag(FIXTURES / "toy_dag.py").topological_order()

    diagram = to_mermaid(
        outer_order,
        subgraphs={
            "suite_02": (middle_order, None, {"transform_a": (innermost_order, None)}),
        },
    )

    # Three real nesting levels: outer -> suite_02 -> transform_a -> extract.
    assert 'subgraph t_suite_02 ["suite_02"]' in diagram
    assert 'subgraph t_suite_02__t_transform_a ["transform_a"]' in diagram
    assert "t_suite_02__t_transform_a__t_extract" in diagram
    assert (
        "t_suite_02__t_transform_a__t_extract --> t_suite_02__t_transform_a__t_transform_a"
        in diagram
    )
    # A sibling at the middle level with no further nesting stays a plain node.
    assert 't_suite_02__t_extract["extract"]' in diagram
    # The existing 2-element shape still works unchanged alongside the new one.
    diagram_2tuple = to_mermaid(outer_order, subgraphs={"suite_02": (middle_order, None)})
    assert 'subgraph t_suite_02 ["suite_02"]' in diagram_2tuple
    assert "subgraph" not in diagram_2tuple.replace('subgraph t_suite_02 ["suite_02"]', "", 1)
