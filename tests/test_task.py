from duckpipe.task import task


def test_direct_call_runs_underlying_function():
    @task
    def double(x=3):
        return x * 2

    assert double(x=5) == 10
    assert double.name == "double"


def test_dependency_inferred_from_default_value():
    @task
    def a():
        return 1

    @task
    def b(x=a):
        return x + 1

    assert b.upstream_params() == {"x": a}
    assert b.upstream_tasks() == [a]


def test_no_dependency_when_no_task_default():
    @task
    def c(x=1, y="hello"):
        return x

    assert c.upstream_params() == {}
    assert c.upstream_tasks() == []


def test_depends_on_escape_hatch_for_side_effect_tasks():
    @task
    def send_alert():
        return None

    @task(depends_on=[send_alert])
    def cleanup():
        return None

    assert cleanup.upstream_tasks() == [send_alert]
    assert cleanup.upstream_params() == {}


def test_var_args_never_eligible_for_inference():
    @task
    def upstream():
        return 1

    @task
    def sink(*args, **kwargs):
        return args

    # *args/**kwargs can never carry a Task default (SyntaxError in plain
    # Python), so inference naturally never applies to them.
    assert sink.upstream_tasks() == []


def test_task_name_override():
    @task(name="custom_name")
    def original_name():
        return 1

    assert original_name.name == "custom_name"


def test_duplicate_default_across_calls_returns_same_task_object():
    @task
    def shared():
        return 1

    @task
    def x(v=shared):
        return v

    @task
    def y(v=shared):
        return v

    assert x.upstream_tasks() == y.upstream_tasks() == [shared]
