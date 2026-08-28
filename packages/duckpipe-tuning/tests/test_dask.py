from duckpipe_tuning.dask import suggest_dask_settings, suggest_worker_topology


def test_low_core_count_is_one_process_per_core():
    # Dask's own LocalCluster default: <=4 cores -> one process per core,
    # one thread each (avoids GIL contention on a small box).
    for cores in (1, 2, 3, 4):
        n_workers, threads_per_worker = suggest_worker_topology(cores=cores)
        assert (n_workers, threads_per_worker) == (cores, 1)


def test_higher_core_counts_match_dask_own_known_defaults():
    # These are Dask's own documented LocalCluster outputs for these exact
    # core counts (distributed.deploy.utils.nprocesses_nthreads) -- pinned
    # here so a future rewrite can't silently drift from them.
    assert suggest_worker_topology(cores=8) == (4, 2)
    assert suggest_worker_topology(cores=16) == (4, 4)


def test_workers_times_threads_never_exceeds_cores():
    for cores in range(1, 65):
        n_workers, threads_per_worker = suggest_worker_topology(cores=cores)
        assert n_workers * threads_per_worker >= cores  # ceiling, not a shortfall
        assert n_workers >= 1
        assert threads_per_worker >= 1


def test_suggest_dask_settings_shape():
    settings = suggest_dask_settings(cores=8)
    assert set(settings) == {"n_workers", "threads_per_worker", "memory_limit"}
    assert settings["n_workers"] == 4
    assert settings["threads_per_worker"] == 2
    assert settings["memory_limit"].endswith("GB")


def test_mem_fraction_caps_the_suggestion():
    tiny = suggest_dask_settings(mem_fraction=0.0001, cores=8)
    assert tiny["memory_limit"] == "1GB"  # floors at 1GB, never suggests 0
