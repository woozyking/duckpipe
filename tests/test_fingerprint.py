from duckpipe.fingerprint import fingerprint_task, resolve_fingerprints
from duckpipe.task import task


def test_fingerprint_is_deterministic_for_the_same_task():
    @task
    def a():
        return 1

    assert fingerprint_task(a, {}) == fingerprint_task(a, {})


def test_source_change_changes_fingerprint():
    @task
    def a():
        return 1

    @task
    def a2():
        return 2

    assert fingerprint_task(a, {}) != fingerprint_task(a2, {})


def test_config_change_changes_fingerprint():
    @task
    def a():
        return 1

    @task(retries=3)
    def a_retries():
        return 1

    assert fingerprint_task(a, {}) != fingerprint_task(a_retries, {})


def test_upstream_fingerprint_change_propagates():
    @task
    def same():
        return 1

    fp1 = fingerprint_task(same, {"upstream": "aaa"})
    fp2 = fingerprint_task(same, {"upstream": "bbb"})
    assert fp1 != fp2


def test_extra_fingerprint_opts_in_external_state():
    @task(extra_fingerprint=["v1"])
    def watches_external():
        return 1

    @task(extra_fingerprint=["v2"])
    def watches_external_changed():
        return 1

    assert fingerprint_task(watches_external, {}) != fingerprint_task(watches_external_changed, {})


def test_resolve_fingerprints_is_stable_across_two_runs():
    @task
    def extract():
        return 1

    @task
    def transform(x=extract):
        return x

    order = [extract, transform]
    fp_a = resolve_fingerprints(order)
    fp_b = resolve_fingerprints(order)
    assert fp_a == fp_b
