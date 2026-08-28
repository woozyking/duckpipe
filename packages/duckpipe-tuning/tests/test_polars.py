from duckpipe_tuning.polars import suggest_polars_settings, suggest_thread_count


def test_suggest_thread_count_is_a_positive_int():
    assert suggest_thread_count() >= 1
    assert isinstance(suggest_thread_count(), int)


def test_suggest_polars_settings_shape():
    settings = suggest_polars_settings()
    assert set(settings) == {"POLARS_MAX_THREADS"}
    assert isinstance(settings["POLARS_MAX_THREADS"], int)
    assert settings["POLARS_MAX_THREADS"] >= 1


def test_suggest_polars_settings_honors_explicit_threads():
    settings = suggest_polars_settings(threads=3)
    assert settings["POLARS_MAX_THREADS"] == 3
