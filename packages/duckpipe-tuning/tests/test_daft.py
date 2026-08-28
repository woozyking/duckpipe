from duckpipe_tuning.daft import suggest_num_threads


def test_suggest_num_threads_is_a_positive_int():
    assert suggest_num_threads() >= 1
    assert isinstance(suggest_num_threads(), int)


def test_cap_is_honored_on_high_core_count_hosts():
    assert suggest_num_threads(cap=4) <= 4


def test_default_cap_is_never_exceeded():
    from duckpipe_tuning.daft import _MAX_USEFUL_THREADS

    assert suggest_num_threads() <= _MAX_USEFUL_THREADS
