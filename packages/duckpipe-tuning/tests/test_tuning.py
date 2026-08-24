from duckpipe_tuning import suggest_duckdb_settings, suggest_temp_dir_limit, suggest_thread_count


def test_suggest_thread_count_is_a_positive_int():
    assert suggest_thread_count() >= 1
    assert isinstance(suggest_thread_count(), int)


def test_hyperthreading_discount_never_exceeds_logical_count():
    with_discount = suggest_thread_count(hyperthreading_discount=True)
    without_discount = suggest_thread_count(hyperthreading_discount=False)
    assert with_discount <= without_discount


def test_suggest_duckdb_settings_shape():
    settings = suggest_duckdb_settings()
    assert set(settings) == {"threads", "memory_limit"}
    assert isinstance(settings["threads"], int)
    assert settings["memory_limit"].endswith("GB")


def test_join_workload_never_suggests_more_memory_than_aggregation_at_same_threads():
    threads = 4
    agg = suggest_duckdb_settings(workload="aggregation", threads=threads, mem_fraction=1.0)
    join = suggest_duckdb_settings(workload="join", threads=threads, mem_fraction=1.0)

    agg_gb = int(agg["memory_limit"].removesuffix("GB"))
    join_gb = int(join["memory_limit"].removesuffix("GB"))
    assert agg_gb <= join_gb


def test_mem_fraction_caps_the_suggestion_regardless_of_workload():
    tiny = suggest_duckdb_settings(mem_fraction=0.0001, workload="join", threads=64)
    assert tiny["memory_limit"] == "1GB"  # floors at 1GB, never suggests 0


def test_suggest_temp_dir_limit_returns_a_gb_string(tmp_path):
    limit = suggest_temp_dir_limit(tmp_path)
    assert limit.endswith("GB")
    assert int(limit.removesuffix("GB")) >= 1


def test_helpers_never_import_duckdb():
    import sys

    import duckpipe_tuning  # noqa: F401

    assert "duckdb" not in sys.modules
