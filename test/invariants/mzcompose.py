# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Multi-threaded correctness test: scenarios whose invariants hold no matter
which concurrent actions succeed, fail, or end up in an unknown state (e.g.
bank transfers that conserve the total balance), checked continuously while
toxiproxy disrupts the envd<->clusterd, metadata-store, source, and sink
connections, and strictly again after healing.
"""

import random
import subprocess
import threading
import time

from materialize.invariants.executor import Runner
from materialize.invariants.framework import (
    COMPLEXITIES,
    Endpoints,
    EventLog,
    ScenarioContext,
)
from materialize.invariants.scenarios import SCENARIOS
from materialize.invariants.toxiproxy import Leg, ProcessTarget, Proxy, ToxiproxyApi
from materialize.mzcompose.composition import Composition, WorkflowArgumentParser
from materialize.mzcompose.services.clusterd import Clusterd
from materialize.mzcompose.services.kafka import Kafka
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.minio import Minio
from materialize.mzcompose.services.mysql import MySql
from materialize.mzcompose.services.postgres import Postgres, PostgresMetadata
from materialize.mzcompose.services.sql_server import SqlServer
from materialize.mzcompose.services.toxiproxy import Toxiproxy

# Host port for the Kafka listener the harness (producers, consumers) uses.
# It must be a fixed port because Kafka advertises it back to clients.
KAFKA_HOST_PORT = 30993


def materialized_service(image: str | None = None) -> Materialized:
    return Materialized(
        image=image,
        # Route the metadata store (persist consensus and the timestamp
        # oracle) and the persist blob store through toxiproxy. The proxies
        # must exist before this service starts.
        external_metadata_store="toxiproxy",
        metadata_store="postgres-metadata",
        external_blob_store="toxiproxy",
        support_external_clusterd=True,
        sanity_restart=False,
        # Fixed host ports: the disruptor kills this container and a
        # recreation under ephemeral ports would strand every client on the
        # old mapping.
        ports=["16875:6875", "16876:6876", "16877:6877"],
        # Crashes (both the disruptor's kills and genuine panics) restart
        # envd so the invariants keep being verified afterwards. Panics
        # still fail the job via the CI log annotator.
        restart="on-failure",
        default_replication_factor=1,
        additional_system_parameter_defaults={
            "unsafe_enable_unorchestrated_cluster_replicas": "true",
            "allow_real_time_recency": "true",
        },
        depends_on=["toxiproxy"],
    )


def clusterd_service(name: str, image: str | None = None) -> Clusterd:
    return Clusterd(
        name=name,
        image=image,
        # The controller connects through toxiproxy, so the host in request
        # URIs doesn't match this clusterd's hostname.
        environment_extra=["CLUSTERD_GRPC_HOST="],
        # halt! is a designed recovery path for clusterd.
        restart="on-failure",
        # Bounded so that runaway memory (e.g. buffering while the blob leg
        # is cut) kills and restarts this container instead of taking down
        # the whole agent.
        memory="6GB",
    )


CLUSTERD_NAMES = ["clusterd-compute", "clusterd-compute2", "clusterd-storage"]

SERVICES = [
    Toxiproxy(),
    PostgresMetadata(),
    Minio(setup_materialize=True),
    materialized_service(),
    *(clusterd_service(name) for name in CLUSTERD_NAMES),
    # Second replica of the compute cluster, behind its own leg: peeks keep
    # being served (and verified) while one replica is disrupted, and a
    # diverging replica shows up as wrong answers.
    Postgres(),
    MySql(),
    SqlServer(),
    # Kafka clients follow advertised listener addresses after bootstrap, so
    # proxying the bootstrap address alone would not carry any traffic.
    # Instead, dedicated source/sink listeners advertise the toxiproxy
    # address, keeping all of Materialize's Kafka traffic on the proxied
    # legs, while the harness uses the direct HOST listener.
    Kafka(
        ports=[f"{KAFKA_HOST_PORT}:{KAFKA_HOST_PORT}", 9092, 9192, 9096],
        allow_host_ports=True,
        advertised_listeners=[
            "SOURCE://toxiproxy:9092",
            "SINK://toxiproxy:9192",
            f"HOST://localhost:{KAFKA_HOST_PORT}",
            "PLAINTEXT://kafka:9096",
        ],
        environment_extra=[
            "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP="
            "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,SOURCE:PLAINTEXT,"
            "SINK:PLAINTEXT,HOST:PLAINTEXT",
        ],
    ),
]

# One leg per connection of the system under test. All proxies are created
# up front (unused ones are harmless), scenarios pick which legs to disrupt.
LEGS = {
    "metadata": Leg(
        "metadata",
        (Proxy("metadata", 26257, "postgres-metadata:26257"),),
        # Full cuts freeze the coordinator loop (group commit blocks on
        # the oracle), so nothing is served mid-outage and the interesting
        # coverage is at the heal edges. Short cuts buy more edges per run,
        # and stay far below the 15 minute persist lease expiry.
        max_outage=45.0,
    ),
    "clusterd-compute": Leg(
        "clusterd-compute",
        (
            Proxy("compute_storagectl", 2100, "clusterd-compute:2100"),
            Proxy("compute_computectl", 2101, "clusterd-compute:2101"),
        ),
    ),
    "clusterd-compute2": Leg(
        "clusterd-compute2",
        (
            Proxy("compute2_storagectl", 2200, "clusterd-compute2:2100"),
            Proxy("compute2_computectl", 2201, "clusterd-compute2:2101"),
        ),
    ),
    "clusterd-storage": Leg(
        "clusterd-storage",
        (
            Proxy("storage_storagectl", 3100, "clusterd-storage:2100"),
            Proxy("storage_computectl", 3101, "clusterd-storage:2101"),
        ),
    ),
    # Persist blob I/O of envd and every clusterd. Capped like the metadata
    # leg, and restricted to non-buffering toxics: latency and bandwidth
    # would hold the entire blob throughput in toxiproxy's memory.
    "blob": Leg(
        "blob",
        (Proxy("blob", 9000, "minio:9000"),),
        max_outage=45.0,
        kinds=("disable", "timeout", "limit_data"),
    ),
    "pg": Leg("pg", (Proxy("pg", 5432, "postgres:5432"),)),
    "mysql": Leg("mysql", (Proxy("mysql", 3306, "mysql:3306"),)),
    "sql-server": Leg("sql-server", (Proxy("sql_server", 1433, "sql-server:1433"),)),
    "kafka-source": Leg("kafka-source", (Proxy("kafka_source", 9092, "kafka:9092"),)),
    "kafka-sink": Leg("kafka-sink", (Proxy("kafka_sink", 9192, "kafka:9192"),)),
}

# Sources and sinks run in `storage`, MVs and indexes in `compute`, both on
# unorchestrated clusterd containers whose controller connections go through
# toxiproxy. The built-in quickstart cluster stays undisrupted as an
# independent observer path for checkers.
CLUSTER_SETUP_SQL = """
CREATE CLUSTER storage REPLICAS (r1 (
    STORAGECTL ADDRESSES ['toxiproxy:3100'],
    STORAGE ADDRESSES ['clusterd-storage:2103'],
    COMPUTECTL ADDRESSES ['toxiproxy:3101'],
    COMPUTE ADDRESSES ['clusterd-storage:2102'],
    WORKERS 1
));
CREATE CLUSTER compute REPLICAS (
    r1 (
        STORAGECTL ADDRESSES ['toxiproxy:2100'],
        STORAGE ADDRESSES ['clusterd-compute:2103'],
        COMPUTECTL ADDRESSES ['toxiproxy:2101'],
        COMPUTE ADDRESSES ['clusterd-compute:2102'],
        WORKERS 1
    ),
    r2 (
        STORAGECTL ADDRESSES ['toxiproxy:2200'],
        STORAGE ADDRESSES ['clusterd-compute2:2103'],
        COMPUTECTL ADDRESSES ['toxiproxy:2201'],
        COMPUTE ADDRESSES ['clusterd-compute2:2102'],
        WORKERS 1
    )
);
GRANT ALL ON CLUSTER storage TO materialize;
GRANT ALL ON CLUSTER compute TO materialize;
"""


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    parser.add_argument(
        "--scenario",
        default="all",
        choices=["all", *SCENARIOS.keys(), *REPROS.keys()],
    )
    parser.add_argument(
        "--complexity", default="medium", choices=list(COMPLEXITIES.keys())
    )
    parser.add_argument(
        "--runtime",
        default=600,
        type=int,
        help="chaos phase duration per scenario, seconds",
    )
    parser.add_argument("--seed", type=str, default=str(int(time.time())))
    parser.add_argument(
        "--upgrade-from",
        type=str,
        default=None,
        help="start Materialize on this image and swap to the current build"
        " mid-chaos, an upgrade under load and disruptions",
    )
    parser.add_argument(
        "--no-disruptions",
        action="store_true",
        help="run the workload and checkers without any disruptions",
    )
    args = parser.parse_args()

    print(f"--- Random seed is {args.seed}")
    if args.scenario in REPROS:
        log = EventLog("invariants-events.log")
        try:
            with c.test_case(args.scenario):
                run_repro(c, args.scenario, args, log)
        finally:
            log.close()
        return
    names = list(SCENARIOS.keys()) if args.scenario == "all" else [args.scenario]
    log = EventLog("invariants-events.log")
    try:
        for name in names:
            with c.test_case(name):
                run_scenario(c, name, args, log)
    finally:
        log.close()


def run_scenario(c: Composition, name: str, args, log: EventLog) -> None:
    scenario_class = SCENARIOS[name]
    complexity = COMPLEXITIES[args.complexity]
    # Deterministic per (seed, scenario) so single-scenario runs reproduce
    # the same sequences as the same scenario within an `all` run.
    rng = random.Random(f"{args.seed}-{name}")
    log.log(
        "scenario",
        f"starting {name} complexity={complexity.name} runtime={args.runtime}s"
        f" seed={args.seed}",
    )
    c.down(destroy_volumes=True)

    # restart on-failure: proxies live in toxiproxy's memory, so after a
    # crash the disruptor's heal re-creates them (it cannot resurrect the
    # container itself).
    version_services = []
    if args.upgrade_from:
        version_services = [materialized_service(image=args.upgrade_from)] + [
            clusterd_service(name, image=args.upgrade_from) for name in CLUSTERD_NAMES
        ]
    with c.override(
        Toxiproxy(seed=rng.randrange(2**63), restart="on-failure"),
        *version_services,
    ):
        # The proxies must exist before materialized boots: its consensus and
        # timestamp-oracle URLs point at toxiproxy.
        c.up("toxiproxy")
        toxiproxy = ToxiproxyApi(f"http://127.0.0.1:{c.default_port('toxiproxy')}")
        for leg in LEGS.values():
            for proxy in leg.proxies:
                toxiproxy.create(proxy)

        services = [
            "materialized",
            "clusterd-compute",
            "clusterd-compute2",
            "clusterd-storage",
        ]
        services += scenario_class.services
        c.up(*services)
        c.sql(CLUSTER_SETUP_SQL, port=6877, user="mz_system")

        endpoints = Endpoints(
            mz_host="127.0.0.1",
            mz_port=c.default_port("materialized"),
            mz_system_port=c.port("materialized", 6877),
            pg_port=(
                c.default_port("postgres")
                if "postgres" in scenario_class.services
                else None
            ),
            mysql_port=(
                c.default_port("mysql") if "mysql" in scenario_class.services else None
            ),
            sqlserver_port=(
                c.default_port("sql-server")
                if "sql-server" in scenario_class.services
                else None
            ),
            kafka_bootstrap=(
                f"localhost:{KAFKA_HOST_PORT}"
                if "kafka" in scenario_class.services
                else None
            ),
        )
        ctx = ScenarioContext(
            endpoints=endpoints,
            complexity=complexity,
            rng=rng,
            log=log,
            seed=args.seed,
        )
        scenario = scenario_class(ctx)
        scenario.setup()
        legs = [] if args.no_disruptions else [LEGS[n] for n in scenario_class.legs]
        midrun = make_upgrade_swap(c) if args.upgrade_from else None
        try:
            Runner(
                scenario,
                args.runtime,
                toxiproxy,
                legs,
                process_targets(c),
                midrun_event=midrun,
            ).run()
        finally:
            scenario.teardown()


def make_upgrade_swap(c: Composition):
    def swap() -> None:
        # Kill the old-version processes and bring everything back on the
        # current build: an upgrade in the middle of load and disruptions,
        # with the invariants never pausing.
        names = [
            "materialized",
            "clusterd-compute",
            "clusterd-compute2",
            "clusterd-storage",
        ]
        c.kill(*names)
        c.up(*names, detach=True, max_tries=3)

    return swap


def process_targets(c: Composition) -> list[ProcessTarget]:
    """Processes the disruptor may kill or pause.

    The clusterd containers restart automatically (restart on-failure), so
    their heal is an idempotent up(). Upstream databases are deliberately
    absent: they run with fsync disabled, so a SIGKILL could lose committed
    data and invalidate the oracle.
    """

    def target(name: str, max_outage: float = 120.0) -> ProcessTarget:
        return ProcessTarget(
            name=name,
            max_outage=max_outage,
            kill=lambda: c.kill(name),
            # Bounded retries: an up that cannot succeed (e.g. dead
            # proxies) must not wedge the disruptor for minutes, but a
            # single attempt can race a crash-looping restart policy.
            heal=lambda: c.up(name, detach=True, max_tries=3),
            pause=lambda: c.pause(name),
            unpause=lambda: c.unpause(name),
        )

    return [
        target("clusterd-compute"),
        target("clusterd-compute2"),
        target("clusterd-storage"),
        # A paused envd freezes everything like a metadata cut, so its
        # outages are short for the same edge-coverage reason.
        target("materialized", max_outage=45.0),
    ]


# Concentrated reproducers for the open findings, selectable via
# --scenario=repro-*. blob-memory and postheal-stall are deterministic
# sequences, per10 and durable-resume are fixed-sequence loops over races.
REPROS = {
    "repro-blob-memory": "unbounded clusterd memory while the blob store is cut",
    "repro-postheal-stall": "writes stalled long after a metadata cut healed",
    "repro-per10": "persist GC panic: earliest state without rollup",
    "repro-durable-resume": "resumed SUBSCRIBE cancels its carried state",
    "repro-compute-asof": "compute halts hydrating a dataflow past its as_of",
}


def run_repro(c: Composition, name: str, args, log: EventLog) -> None:
    from materialize.invariants.scenarios.table_bank import TableBank

    c.down(destroy_volumes=True)
    rng = random.Random(f"{args.seed}-{name}")
    with c.override(Toxiproxy(seed=rng.randrange(2**63), restart="on-failure")):
        c.up("toxiproxy")
        toxiproxy = ToxiproxyApi(f"http://127.0.0.1:{c.default_port('toxiproxy')}")
        for leg in LEGS.values():
            for proxy in leg.proxies:
                toxiproxy.create(proxy)
        c.up(
            "materialized", "clusterd-compute", "clusterd-compute2", "clusterd-storage"
        )
        c.sql(CLUSTER_SETUP_SQL, port=6877, user="mz_system")
        ctx = ScenarioContext(
            endpoints=Endpoints(
                mz_host="127.0.0.1",
                mz_port=c.default_port("materialized"),
                mz_system_port=c.port("materialized", 6877),
            ),
            complexity=COMPLEXITIES["medium"],
            rng=rng,
            log=log,
            seed=args.seed,
        )
        scenario = TableBank(ctx)
        scenario.setup()
        bundles = [
            scenario.make_worker(i, random.Random(rng.randrange(2**31)))
            for i in range(4)
        ]
        stop = threading.Event()

        def drive(bundle) -> None:
            ticks = 0
            while not stop.is_set():
                ticks += 1
                # LedgerTransfer for steady writes (consensus churn), plus
                # DdlChurn: created and dropped MVs mean shard finalization,
                # which is what keeps persist GC busy.
                action = bundle.actions[3] if ticks % 40 == 0 else bundle.actions[1]
                try:
                    action.run()
                except Exception:
                    pass
                stop.wait(0.005)

        threads = [
            threading.Thread(target=drive, args=(b,), daemon=True) for b in bundles
        ]
        for t in threads:
            t.start()
        try:
            REPRO_FUNCS[name](c, toxiproxy, ctx, log)
        finally:
            stop.set()
            for t in threads:
                t.join(timeout=10)


def _toxiproxy_heal(toxiproxy: ToxiproxyApi, log: EventLog) -> None:
    """Heal everything, surviving a toxiproxy crash-restart in between.

    A restarted toxiproxy comes back empty, so besides retrying the reset
    the legs' proxies are re-created. Mirrors the disruptor's heal path.
    """
    deadline = time.monotonic() + 60
    while True:
        try:
            toxiproxy.reset()
            existing = toxiproxy.proxies()
            for leg in LEGS.values():
                for proxy in leg.proxies:
                    if proxy.name not in existing:
                        log.log("repro", f"re-creating lost proxy {proxy.name}")
                        toxiproxy.create(proxy)
            return
        except Exception as e:
            if time.monotonic() > deadline:
                subprocess.run(
                    ["docker", "logs", "--tail", "20", "invariants-toxiproxy-1"]
                )
                raise AssertionError(f"toxiproxy did not heal: {e}") from e
            time.sleep(2)


def _rss_gb(container: str) -> float:
    out = (
        subprocess.check_output(
            ["docker", "stats", "--no-stream", "--format", "{{.MemUsage}}", container],
            text=True,
        )
        .split("/")[0]
        .strip()
    )
    for unit, mult in [("GiB", 1.0), ("MiB", 1 / 1024), ("KiB", 1 / 1048576)]:
        if out.endswith(unit):
            return float(out[: -len(unit)]) * mult
    return 0.0


def repro_blob_memory(c, toxiproxy, ctx, log) -> None:
    """Cut the blob store under write load, watch process memory grow."""
    containers = [
        "invariants-materialized-1",
        "invariants-clusterd-compute-1",
        "invariants-clusterd-compute2-1",
        "invariants-clusterd-storage-1",
    ]
    # Let hydration finish and take the settled baseline as the minimum of a
    # few samples, memory right after startup spikes.
    time.sleep(20)
    base = {name: _rss_gb(name) for name in containers}
    for _ in range(2):
        time.sleep(5)
        for name in containers:
            base[name] = min(base[name], _rss_gb(name))
    log.log(
        "repro", "baseline: " + ", ".join(f"{n}={v:.2f}GiB" for n, v in base.items())
    )
    toxiproxy.set_enabled("blob", False)
    try:
        for _ in range(24):
            time.sleep(10)
            now = {name: _rss_gb(name) for name in containers}
            log.log(
                "repro",
                "blob cut: " + ", ".join(f"{n}={v:.2f}GiB" for n, v in now.items()),
            )
            for name in containers:
                if now[name] > base[name] + 1.5:
                    raise AssertionError(
                        f"REPRODUCED: {name} memory grew {base[name]:.2f} ->"
                        f" {now[name]:.2f}GiB while the blob store was"
                        " unavailable (unbounded buffering)"
                    )
    finally:
        _toxiproxy_heal(toxiproxy, log)
    log.log("repro", "not reproduced within 240s")


def repro_postheal_stall(c, toxiproxy, ctx, log) -> None:
    """Stall writes by making the metadata store slow under heavy catalog churn.

    group_commit runs inline on the single-threaded coordinator loop and
    blocks on metadata round trips: the timestamp oracle (get_local_write_ts)
    and the catalog-shard consensus op (catalog.advance_upper). A clean
    cut/heal recovers in seconds because a local postgres is fast, so this
    models the overloaded CI metadata store with a latency toxic that delays
    every round trip.

    Both the latency and the load matter. Latency alone slows each write by
    only a handful of round trips, and the coordinator loop is biased to
    prioritize group commit over DDL, so DDL does not starve writes by
    queuing. The DROP-heavy materialized-view churn instead grows the catalog
    shard's persist state, so each delayed metadata op does more work and a
    single group commit blocks the loop for tens of seconds. Measured
    empirically: ~2.5s/direction plus this churn stalls writes ~150s while
    envd keeps accepting connections. Time-to-first-committed-write is
    measured while the store is slow.
    """
    from materialize.invariants.framework import Outcome
    from materialize.invariants.mz import MzClient

    # Per-direction delay on the metadata leg. Well below the stall threshold
    # on its own: exceeding it is the churn-driven amplification (each catalog
    # metadata op does many delayed round trips), not one slow round trip.
    METADATA_LATENCY_MS = 2500
    STALL_THRESHOLD_S = 120.0

    stop = threading.Event()

    def writer(idx: int) -> None:
        client = MzClient(ctx, f"stall-writer-{idx}")
        seq = 0
        while not stop.is_set():
            seq += 1
            try:
                client.write(
                    f"INSERT INTO ledger VALUES ({1000 + idx}, {seq}, 0, 0)",
                    timeout=20,
                )
            except Exception:
                pass
            stop.wait(0.002)

    def churner(idx: int) -> None:
        client = MzClient(ctx, f"stall-churn-{idx}")
        seq = 0
        while not stop.is_set():
            seq += 1
            # CREATE and DROP hammer the catalog shard, the same shard
            # group_commit's advance_upper operates on. The churn grows that
            # shard's persist state, so under a slow leg each catalog metadata
            # op does more delayed round trips and group_commit blocks the
            # coordinator loop for tens of seconds. Unique names plus IF
            # EXISTS keep the loop idempotent across UNKNOWN outcomes.
            name = f"stall_mv_{idx}_{seq}"
            try:
                client.write(
                    f"CREATE MATERIALIZED VIEW {name} IN CLUSTER compute"
                    " AS SELECT count(*) AS cnt FROM ledger",
                    timeout=20,
                )
                client.write(f"DROP MATERIALIZED VIEW IF EXISTS {name}", timeout=20)
            except Exception:
                pass
            stop.wait(0.02)

    threads = [
        threading.Thread(target=writer, args=(i,), daemon=True) for i in range(8)
    ] + [threading.Thread(target=churner, args=(i,), daemon=True) for i in range(12)]
    for t in threads:
        t.start()
    try:
        # Let the load reach steady state and the catalog shard get hot
        # before the store slows down.
        time.sleep(20)
        # Model the overloaded metadata store: delay both directions so every
        # oracle and consensus round trip costs seconds.
        for proxy in LEGS["metadata"].proxies:
            for stream in ("downstream", "upstream"):
                toxiproxy.add_toxic(
                    proxy.name,
                    f"latency-{stream}",
                    "latency",
                    {
                        "latency": METADATA_LATENCY_MS,
                        "jitter": METADATA_LATENCY_MS // 2,
                    },
                    stream=stream,
                )
        log.log(
            "repro",
            f"metadata slowed (~{METADATA_LATENCY_MS}ms/direction), probing"
            " write liveness",
        )
        # A per-attempt timeout above the threshold so one attempt can observe
        # a commit anywhere up to the threshold and report its true latency.
        probe_timeout = STALL_THRESHOLD_S + 10
        client = MzClient(ctx, "stall-probe")
        start = time.monotonic()
        while time.monotonic() - start < 300:
            if (
                client.write(
                    "INSERT INTO ledger VALUES (-3, 0, 0, 0)", timeout=probe_timeout
                )
                == Outcome.COMMITTED
            ):
                took = time.monotonic() - start
                log.log("repro", f"first write committed {took:.1f}s into slowdown")
                if took > STALL_THRESHOLD_S:
                    raise AssertionError(
                        f"REPRODUCED: writes stalled {took:.1f}s while the"
                        " metadata store was slow"
                    )
                return
        raise AssertionError(
            "REPRODUCED: no write committed within 300s of the metadata store"
            " slowing down"
        )
    finally:
        _toxiproxy_heal(toxiproxy, log)
        stop.set()
        for t in threads:
            t.join(timeout=10)


def repro_per10(c, toxiproxy, ctx, log) -> None:
    """Blob cut, heal, immediate kill: the PER-10 panic window.

    A ~1%-per-cycle race in CI, so this loops tightly: short cuts, kills
    alternating between envd and the clusterds, DDL churn in the load for
    shard-finalization GC pressure, and the panic grepped in all processes.
    """
    targets = ["materialized", "clusterd-compute", "clusterd-storage"]
    for iteration in range(30):
        toxiproxy.set_enabled("blob", False)
        time.sleep(5)
        _toxiproxy_heal(toxiproxy, log)
        time.sleep(2)
        victim = targets[iteration % len(targets)]
        c.kill(victim)
        c.up(victim, detach=True, max_tries=3)
        time.sleep(5)
        for name in targets + ["clusterd-compute2"]:
            logs = c.invoke("logs", name, capture=True).stdout or ""
            if "did not have corresponding rollup" in logs:
                raise AssertionError(
                    f"REPRODUCED PER-10 in {name}, iteration {iteration}"
                )
        log.log("repro", f"iteration {iteration} (killed {victim}): no panic yet")
    log.log("repro", "not reproduced in 30 iterations")


def repro_durable_resume(c, toxiproxy, ctx, log) -> None:
    """Blob cut + envd kill, then a durable resume must keep its state."""
    from materialize.invariants.checkers import SubscribeChecker
    from materialize.invariants.framework import InvariantViolation

    total = ctx.complexity.accounts * 1000

    class ResumeChecker(SubscribeChecker):
        def __init__(self, rng: random.Random) -> None:
            super().__init__(
                rng, ctx, "resume-repro", "SELECT total FROM total", durable=True
            )

        def validate_state(self, state: dict, ts: int) -> None:
            got = {(int(k[0]),): v for k, v in state.items()}
            if got != {(total,): 1}:
                raise InvariantViolation(f"state {got} at {ts}")

    for iteration in range(5):
        rng = random.Random(f"resume-{iteration}")
        checker = ResumeChecker(rng)
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            try:
                checker.check_once()
            except Exception:
                pass
        assert checker.last_validated_ts is not None
        toxiproxy.set_enabled("blob", False)
        time.sleep(15)
        _toxiproxy_heal(toxiproxy, log)
        time.sleep(2)
        c.kill("materialized")
        c.up("materialized", detach=True, max_tries=3)
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            try:
                checker.check_once()  # raises InvariantViolation if reproduced
            except AssertionError:
                raise AssertionError(
                    f"REPRODUCED durable-resume anomaly, iteration {iteration}"
                )
            except Exception:
                pass
        checker.close()
        log.log("repro", f"iteration {iteration}: resume stayed consistent")
    log.log("repro", "not reproduced in 5 iterations")


def repro_compute_asof(c, toxiproxy, ctx, log) -> None:
    """Compute halts hydrating a dataflow whose input compacted past its as_of.

    incidents-and-escalations #39 / CLU-34. When the controller loses a
    replica it drops that replica's per-replica read holds, and for a dataflow
    it no longer wants (a canceled peek) it does not reinstall them, while the
    replica still has the dataflow pending. The inputs then compact, and when
    the replica renders the dataflow its as_of is below the input's since, so
    compute_import halts with "cannot serve requested as_of".

    Concentrates that window against the compute cluster's second replica
    (clusterd-compute2, its own control-plane leg): a burst of slow-path peeks
    over the base ledger builds one-off dataflows on both replicas, the leg is
    cut so the controller drops r2's holds while r2 keeps the pending
    dataflows, the peeks are canceled so their global holds drop too, and the
    base ledger has no RETAIN HISTORY, so with nothing holding it its since
    advances to ~now while the leg stays cut. r2 then renders a pending peek at
    a since-passed as_of and halts. The disruption is a leg cut, not a kill: a
    killed replica loses the pending dataflow and would just get fresh as_ofs
    on restart. A race, so it loops and greps r2's log for the halt.
    """
    from materialize.invariants.mz import MzClient

    def set_compute2(enabled: bool) -> None:
        for proxy in LEGS["clusterd-compute2"].proxies:
            toxiproxy.set_enabled(proxy.name, enabled)

    def halted() -> str | None:
        for name in ("clusterd-compute2", "clusterd-compute"):
            logs = c.invoke("logs", name, capture=True).stdout or ""
            if "cannot serve requested as_of" in logs:
                return name
        return None

    def peek(tag: str) -> None:
        cl = MzClient(ctx, f"asof-peek-{tag}")
        try:
            cl.query("SET cluster = compute")
            # A predicate no index serves forces the slow path, a one-off peek
            # dataflow importing the base ledger. The short timeout cancels it,
            # dropping its global read hold.
            cl.query(
                f"SELECT count(*) FROM ledger WHERE amount > {tag} AND account >= 0",
                timeout=3,
            )
        except Exception:
            pass
        cl.reset()

    for iteration in range(15):
        set_compute2(True)
        # Fire the peeks, then cut r2's control plane ~1s in, while the peek
        # dataflows are still installing/rendering on r2.
        threads = [
            threading.Thread(target=peek, args=(f"{iteration}-{i}",), daemon=True)
            for i in range(8)
        ]
        for t in threads:
            t.start()
        time.sleep(1.0)
        set_compute2(False)
        # The peeks cancel at their 3s timeout, dropping the global holds. With
        # r2's per-replica holds already dropped by the disconnect, nothing
        # pins the ledger, so its since advances past the pending as_ofs while
        # the leg stays cut.
        for t in threads:
            t.join(timeout=10)
        time.sleep(40)
        set_compute2(True)
        time.sleep(8)
        name = halted()
        if name is not None:
            raise AssertionError(
                f"REPRODUCED incident #39: {name} halted with 'cannot serve"
                f" requested as_of', iteration {iteration}"
            )
        log.log("repro", f"iteration {iteration}: no as_of halt yet")
    log.log("repro", "not reproduced in 15 iterations")


REPRO_FUNCS = {
    "repro-blob-memory": repro_blob_memory,
    "repro-postheal-stall": repro_postheal_stall,
    "repro-per10": repro_per10,
    "repro-durable-resume": repro_durable_resume,
    "repro-compute-asof": repro_compute_asof,
}
