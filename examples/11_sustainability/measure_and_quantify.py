"""Measures `run_pipeline.py`'s real resource footprint, then quantifies
the one thing this comparison actually isolates: the annual cost of
running a *standing* orchestrator control plane at all, in two real
setups -- self-hosted (a real Prefect-on-Kubernetes setup, inspected
directly, not estimated) or managed (Prefect Cloud, using a *push* work
pool so no worker of your own runs either) -- purely to eventually make
the exact same call DuckPipe can trigger directly from a plain
cron/EventBridge rule, with nothing standing behind it at all.

The actual job -- `run_pipeline()`, a plain function -- is identical in
both arms: same code, same data, same duration, same memory. That's
deliberate (see this folder's README): it isolates the standing-infra
tax as the whole delta, rather than blending it with a separate (and
more contestable) claim about DuckDB being faster than some other
engine. Nothing here argues DuckDB is faster than anything; the engine
is held constant on purpose.

Every constant below is sourced, not guessed -- see the comment next to
each. Run this against the bundled sample for a quick correctness check,
or against a real multi-month `DUCKPIPE_EXAMPLE_DATA` (see pipeline.py's
own docstring) for the numbers this folder's README actually reports.

    uv run python examples/11_sustainability/measure_and_quantify.py
"""

import multiprocessing
import time
from pathlib import Path

import psutil
from run_pipeline import run_pipeline

HERE = Path(__file__).parent

# -- Sourced constants (see README.md for the full citations) ---------------

# Cloud Carbon Footprint's own "Cloud Jewels" methodology (originally
# Etsy's), energy coefficients per vCPU derived from SPECpower committee
# data, AMD EPYC 3rd-gen figures: kW at idle (min) and kW at 100%
# utilization (max). https://www.cloudcarbonfootprint.org/docs/methodology/
CPU_KW_MIN = 4.34e-4  # kW per vCPU at ~0% utilization
CPU_KW_MAX = 1.948e-3  # kW per vCPU at 100% utilization

# A real self-hosted Prefect setup on GKE, inspected directly via
# `kubectl`/`gcloud` (read-only), not estimated: `prefect-server` and
# `prefect-worker` are the only two pods Prefect's own Helm chart needs
# to function at all (Grafana/Loki/Promtail alongside them are optional
# observability add-ons, not counted here), and both run on one
# dedicated node -- a `t2d-standard-4` (4 vCPU). That's the real
# standing floor: not a commonly-cited estimate, an actual `kubectl
# describe node` result.
SELF_HOSTED_STANDING_VCPU = 4
# An always-on-but-mostly-idle service (polling for triggers, occasional
# bursts) -- 15% average utilization is a deliberately conservative
# assumption; a busier scheduler would only make the eliminated cost
# larger, not smaller.
SELF_HOSTED_AVG_UTILIZATION = 0.15

# Data center overhead. Two real, current (2025) figures, used as a
# range rather than picking the flattering one:
PUE_HYPERSCALE = 1.09  # Google's own fleet-wide average, 2025
PUE_INDUSTRY_AVG = 1.54  # Uptime Institute's global survey average, 2025

# AWS Lambda's own documented CPU-to-memory relationship: at 1,769 MB, a
# function gets the equivalent of one full vCPU; CPU scales linearly
# below that. https://docs.aws.amazon.com/lambda/latest/dg/configuration-memory.html
LAMBDA_MB_PER_VCPU = 1769

# AWS Lambda's own list pricing, x86, us-east-1, on-demand (no savings
# plan): https://aws.amazon.com/lambda/pricing/
LAMBDA_PRICE_PER_GB_SECOND_USD = 1.66667e-5
LAMBDA_PRICE_PER_REQUEST_USD = 0.20 / 1_000_000

# The exact node type GKE is running for `prefect-server` + `prefect-
# worker` in that real setup (`kubectl get node -o jsonpath=
# '{.metadata.labels}'`), on-demand, us-central1:
# https://cloudprice.net/gcp/compute/instances/t2d-standard-4
GCP_NODE_HOURLY_USD = 0.169

# The exact metadata-DB shape that real setup points at (`gcloud
# sql instances describe ... --format='value(settings.tier,
# settings.dataDiskSizeGb)'` -> db-custom-2-8192, 25GB): Cloud SQL bills
# per vCPU and per GB of memory, by the hour, zonal (non-HA), plus SSD
# storage by the GB-month. https://cloud.google.com/sql/docs/postgres/pricing
CLOUDSQL_VCPU_HOURLY_USD = 0.0413
CLOUDSQL_GB_HOURLY_USD = 0.007
CLOUDSQL_STORAGE_GB_MONTHLY_USD = 0.22
CLOUDSQL_VCPUS = 2
CLOUDSQL_MEMORY_GB = 8
CLOUDSQL_STORAGE_GB = 25

# Prefect's own published Cloud pricing: https://www.prefect.io/pricing
# -- Starter is a flat rate for up to 3 seats; Team bills per seat past
# a 4-seat minimum. Using a *push* work pool (AWS ECS/Fargate, Google
# Cloud Run, Azure Container Instances -- Prefect Cloud submits directly
# with your cloud credentials) needs no worker of your own at all, so
# these figures aren't padded with one -- this is Prefect's own real
# floor when configured for exactly that, not a worst case.
PREFECT_STARTER_MONTHLY_USD = 100
PREFECT_STARTER_MAX_SEATS = 3
PREFECT_TEAM_PER_SEAT_MONTHLY_USD = 100
PREFECT_TEAM_MIN_SEATS = 4

# Why N separate self-hosted setups exist at all, not an arbitrary round
# number: self-hosted Prefect Server has no native multi-tenant
# workspace isolation the way Prefect Cloud does, and Prefect's own
# community recommends running one Server instance per team as the
# workaround (https://www.prefect.io/prefect/customer-managed) -- the
# same documented pattern as "Airflow sprawl" (Astronomer's own term)
# in that ecosystem. 50 teams is this example's own illustrative org
# size, not a survey result.
TEAMS_EXAMPLE = 50
# Not every team member needs direct platform (seat) access, usually
# just whoever deploys/maintains that team's own flows -- 1-2 seats per
# team is a realistic range, not a per-headcount assumption.
SEATS_PER_TEAM_LOW = 1
SEATS_PER_TEAM_HIGH = 2

RUNS_PER_YEAR = 365  # a daily batch job -- see README.md for why


def _run_pipeline_in_subprocess(q: multiprocessing.Queue) -> None:
    # Module-level, not a closure -- macOS/Windows' default "spawn" start
    # method pickles the target by reference, which a nested function
    # can't be (confirmed the hard way, not assumed: an earlier version
    # of this nested it inside `_measure` and failed outright).
    q.put(run_pipeline())


def _measure() -> tuple[float, int]:
    """Runs `run_pipeline()` in its own process (never the measuring
    one), polling its actual RSS the same way datapunk's own `runner.py`
    watches an engine subprocess -- the measurement this whole
    quantification rests on, not an estimate."""
    q: multiprocessing.Queue = multiprocessing.Queue()
    proc = multiprocessing.Process(target=_run_pipeline_in_subprocess, args=(q,))
    started = time.perf_counter()
    proc.start()

    peak_rss = 0
    ps_proc = psutil.Process(proc.pid)
    while proc.is_alive():
        try:
            rss = ps_proc.memory_info().rss
            for child in ps_proc.children(recursive=True):
                rss += child.memory_info().rss
            peak_rss = max(peak_rss, rss)
        except psutil.NoSuchProcess:
            pass
        time.sleep(0.05)

    proc.join()
    elapsed = time.perf_counter() - started
    result = q.get()
    if not result["success"]:
        raise RuntimeError(f"run_pipeline() failed: {result['statuses']}")
    return elapsed, peak_rss


def _round_up_lambda_memory(peak_bytes: int) -> int:
    """Priced against AWS Lambda's own public billing model as a
    representative stand-in for lean serverless compute generally
    (Cloud Functions, Cloud Run, and Azure Functions all bill similarly)
    -- `run_pipeline()` itself isn't bound to Lambda's calling
    convention, this is just a real, sourced way to size what running it
    somewhere small and ephemeral would cost. A real setup configures a
    fixed memory size in advance -- round the measured peak up to a sane
    real-world tier, with headroom, rather than reporting the exact byte
    count as if billing worked that precisely."""
    peak_mb = peak_bytes / (1024 * 1024)
    tiers = [128, 256, 512, 1024, 1536, 2048, 3008, 4096]
    for tier in tiers:
        if peak_mb <= tier * 0.8:  # leave 20% headroom, not a hairline fit
            return tier
    return tiers[-1]


def _lambda_energy_kwh(memory_mb: int, duration_s: float, pue: float) -> float:
    vcpu_equivalent = memory_mb / LAMBDA_MB_PER_VCPU
    # Lambda runs its allocated CPU share flat-out while executing --
    # there's no "idle" state mid-invocation, so this uses the 100%
    # utilization coefficient, not the interpolated one below.
    kw = vcpu_equivalent * CPU_KW_MAX
    return kw * (duration_s / 3600) * pue


def _self_hosted_standing_kwh_per_year(pue: float) -> float:
    coefficient = CPU_KW_MIN + (CPU_KW_MAX - CPU_KW_MIN) * SELF_HOSTED_AVG_UTILIZATION
    vcpu_hours_per_year = SELF_HOSTED_STANDING_VCPU * 24 * 365
    return vcpu_hours_per_year * coefficient * pue


def _lambda_cost_usd(memory_mb: int, duration_s: float) -> float:
    gb_seconds = (memory_mb / 1024) * duration_s
    return gb_seconds * LAMBDA_PRICE_PER_GB_SECOND_USD + LAMBDA_PRICE_PER_REQUEST_USD


def _standing_usd_per_year(hourly_usd: float) -> float:
    return hourly_usd * 24 * 365


def _self_hosted_usd_per_year() -> float:
    """The real dollar cost of the exact setup `SELF_HOSTED_STANDING_VCPU`
    models: one GKE node running `prefect-server` + `prefect-worker`, plus
    the Cloud SQL instance they point at."""
    node = _standing_usd_per_year(GCP_NODE_HOURLY_USD)
    db_compute = _standing_usd_per_year(
        CLOUDSQL_VCPUS * CLOUDSQL_VCPU_HOURLY_USD + CLOUDSQL_MEMORY_GB * CLOUDSQL_GB_HOURLY_USD
    )
    db_storage = CLOUDSQL_STORAGE_GB * CLOUDSQL_STORAGE_GB_MONTHLY_USD * 12
    return node + db_compute + db_storage


def _prefect_cloud_usd_per_year(seats: int) -> float:
    """Push work pool -- no worker of your own, so this is purely the
    seat-priced subscription: flat under Starter's 3-seat cap, per-seat
    on Team past its 4-seat minimum."""
    monthly = (
        PREFECT_STARTER_MONTHLY_USD
        if seats <= PREFECT_STARTER_MAX_SEATS
        else PREFECT_TEAM_PER_SEAT_MONTHLY_USD * seats
    )
    return monthly * 12


def main() -> None:
    import pipeline as pl

    print(f"sources: {len(pl.SOURCES)} -- running run_pipeline()...")
    elapsed, peak_rss = _measure()
    memory_mb = _round_up_lambda_memory(peak_rss)
    print(f"\nmeasured: {elapsed:.2f}s wall clock, peak RSS {peak_rss / (1024*1024):.1f} MB")
    print(f"rounded up to a real Lambda tier: {memory_mb} MB\n")

    scenarios = (
        ("hyperscale (PUE 1.09)", PUE_HYPERSCALE),
        ("industry avg (PUE 1.54)", PUE_INDUSTRY_AVG),
    )
    for label, pue in scenarios:
        lambda_kwh_per_run = _lambda_energy_kwh(memory_mb, elapsed, pue)
        lambda_kwh_per_year = lambda_kwh_per_run * RUNS_PER_YEAR
        standing_kwh_per_year = _self_hosted_standing_kwh_per_year(pue)
        multiple = standing_kwh_per_year / lambda_kwh_per_year

        print(f"-- {label} --")
        print(f"  one run_pipeline() invocation: {lambda_kwh_per_run * 1000:.3f} Wh")
        print(
            f"  {RUNS_PER_YEAR} runs/year (both arms, identical): "
            f"{lambda_kwh_per_year:.2f} kWh/year"
        )
        print(f"  self-hosted standing infra, 24/7: {standing_kwh_per_year:.2f} kWh/year")
        print(
            f"  => eliminating standing infra saves ~{standing_kwh_per_year:.1f} kWh/year "
            f"per setup ({multiple:.0f}x the actual job's own footprint)"
        )
        for n in (10, 100, 1000):
            print(f"     at {n:>5} such setups: ~{standing_kwh_per_year * n:,.0f} kWh/year")
        print()

    lambda_usd_per_year = _lambda_cost_usd(memory_mb, elapsed) * RUNS_PER_YEAR
    print(
        f"-- dollars: {RUNS_PER_YEAR} run_pipeline() invocations/year "
        f"(both arms, identical): ${lambda_usd_per_year:.2f}/year --\n"
    )

    self_hosted_usd = _self_hosted_usd_per_year()
    print("self-hosted (our own real GKE setup, inspected not estimated):")
    print(f"  prefect-server + prefect-worker + Cloud SQL, 24/7: ${self_hosted_usd:,.0f}/year")
    print("  no native multi-tenant workspace isolation -- one instance per team is")
    print("  Prefect's own community-recommended workaround, not an arbitrary multiplier:")
    for n in (10, TEAMS_EXAMPLE, 1000):
        total = self_hosted_usd * n
        print(f"     at {n:>5} teams, each with their own instance: ~${total:,.0f}/year")
    print()

    per_seat_annual = PREFECT_TEAM_PER_SEAT_MONTHLY_USD * 12
    print(f"Prefect Cloud (managed, push work pool): the same {TEAMS_EXAMPLE} teams, one")
    print(f"  shared workspace instead, at ${per_seat_annual:,.0f}/year per seat --")
    print("  Cloud's own answer to that isolation gap:")
    for seats_per_team in (SEATS_PER_TEAM_LOW, SEATS_PER_TEAM_HIGH):
        seats = TEAMS_EXAMPLE * seats_per_team
        cloud_usd = _prefect_cloud_usd_per_year(seats)
        print(f"     {seats:>3} seats ({seats_per_team}/team): ${cloud_usd:,.0f}/year total")
    print()


if __name__ == "__main__":
    main()
