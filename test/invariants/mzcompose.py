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

import json
import os
import random
import re
import subprocess
import threading
import time
from contextlib import ExitStack
from typing import Any

from materialize.invariants.executor import Runner
from materialize.invariants.framework import (
    COMPLEXITIES,
    Endpoints,
    EventLog,
    InvariantViolation,
    ScenarioContext,
    wait_until,
)
from materialize.invariants.mz import MzClient
from materialize.invariants.scenarios import SCENARIOS
from materialize.invariants.toxiproxy import (
    DISRUPTION_KINDS,
    PROCESS_KINDS,
    Leg,
    ProcessTarget,
    Proxy,
    ToxiproxyApi,
)
from materialize.mzcompose.composition import Composition, WorkflowArgumentParser
from materialize.mzcompose.services.clusterd import Clusterd
from materialize.mzcompose.services.kafka import Kafka
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.minio import Minio, minio_blob_uri
from materialize.mzcompose.services.mysql import MySql
from materialize.mzcompose.services.persistcli import Persistcli
from materialize.mzcompose.services.postgres import Postgres, PostgresMetadata
from materialize.mzcompose.services.schema_registry import SchemaRegistry
from materialize.mzcompose.services.sql_server import SqlServer
from materialize.mzcompose.services.toxiproxy import Toxiproxy
from materialize.version_list import get_latest_published_version

# Host port for the Kafka listener the harness (producers, consumers) uses.
# It must be a fixed port because Kafka advertises it back to clients.
KAFKA_HOST_PORT = 30993

# Local iteration: run the Materialize services from this image instead of
# building the current source. An environment variable, not a workflow
# argument, because the image acquisition happens before workflow arguments
# are parsed.
INVARIANTS_IMAGE = os.environ.get("INVARIANTS_IMAGE")

# Turns every soft assertion in the processes under test into a panic, which
# the crash probe then reports. Our images build with debug assertions off, so
# without this a soft assertion only writes a line to a log. An environment
# variable for the same reason as INVARIANTS_IMAGE: the services are
# constructed before workflow arguments are parsed.
SOFT_ASSERTIONS = os.environ.get("INVARIANTS_SOFT_ASSERTIONS")

SOFT_ASSERTION_ENV = ["MZ_SOFT_ASSERTIONS=1"] if SOFT_ASSERTIONS else []


def materialized_service(image: str | None = None) -> Materialized:
    return Materialized(
        image=image or INVARIANTS_IMAGE,
        environment_extra=SOFT_ASSERTION_ENV,
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
            "enable_refresh_every_mvs": "true",
            "enable_bounded_staleness_isolation": "true",
            "enable_replacement_materialized_views": "true",
            "enable_within_timestamp_order_by_in_subscribe": "true",
            # ShardChurn evolves the schema of its scratch tables, so
            # compaction has to migrate batches written under both.
            "enable_alter_table_add_column": "true",
        },
        depends_on=["toxiproxy"],
    )


# Persist pubsub, one toxiproxy listener per clusterd. Persist's blob and
# consensus URLs are chosen by envd and handed to every clusterd verbatim, so
# those legs can only be cut for all processes at once. Pubsub is the one
# persist connection each clusterd is configured with individually, which
# makes it the only place a *partial* partition is expressible: one process
# stops hearing about other processes' writes while the rest keep hearing
# them, so it falls back to polling and lags behind a system that thinks it
# is fine. Nothing else here produces two peers with different views.
# CPU squeeze for the `throttle` disruption, as a cgroup quota per period.
CPU_PERIOD = 100_000
CPU_THROTTLED = 50_000

PUBSUB_PORTS = {
    "clusterd-compute": 6879,
    "clusterd-compute2": 6880,
    "clusterd-storage": 6881,
}


def clusterd_service(name: str, image: str | None = None) -> Clusterd:
    return Clusterd(
        name=name,
        image=image or INVARIANTS_IMAGE,
        # The controller connects through toxiproxy, so the host in request
        # URIs doesn't match this clusterd's hostname.
        environment_extra=["CLUSTERD_GRPC_HOST=", *SOFT_ASSERTION_ENV],
        persist_pubsub_url=f"http://toxiproxy:{PUBSUB_PORTS[name]}",
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
    # The extra bucket serves the COPY TO S3 checker, reachable for
    # Materialize by container name and for the harness via the host port.
    Minio(setup_materialize=True, additional_directories=["copytos3"]),
    materialized_service(),
    # clusterd-compute2 is the compute cluster's second replica, behind its
    # own leg: peeks keep being served (and verified) while one replica is
    # disrupted, and a diverging replica shows up as wrong answers.
    *(clusterd_service(name) for name in CLUSTERD_NAMES),
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
    # The schema registry talks to Kafka directly (it is harness-side
    # infrastructure for the broker); Materialize reaches it through the
    # csr leg.
    SchemaRegistry(kafka_servers=[("kafka", "9096")]),
    # Idle sidecar, started only for the post-run persist state audit.
    Persistcli(),
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
        # TODO: Reenable the cutting kinds when PER-49 is fixed, as for
        # clusterd-compute2 below. NOTE: this does not remove the trigger.
        # Losing the replica is what drops its per-replica read holds, and the
        # disruptor's SIGKILLs of this process do that just as well as a leg
        # cut, so the halt keeps occurring until PER-49 is fixed. Reproducer:
        # bin/mzcompose --find invariants run default --scenario=repro-compute-asof
        kinds=("latency", "limit_data", "bandwidth"),
    ),
    "clusterd-compute2": Leg(
        "clusterd-compute2",
        (
            Proxy("compute2_storagectl", 2200, "clusterd-compute2:2100"),
            Proxy("compute2_computectl", 2201, "clusterd-compute2:2101"),
        ),
        # TODO: Reenable the cutting kinds when PER-49 is fixed. Losing this
        # replica drops its per-replica read holds while it keeps a pending
        # dataflow, whose inputs then compact past the dataflow's as_of, and
        # rendering it halts the process. Degrading the leg still covers the
        # controller's slow-replica paths. Reproducer:
        # bin/mzcompose --find invariants run default --scenario=repro-compute-asof
        kinds=("latency", "limit_data", "bandwidth"),
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
    # TODO: Shorten to 45s again when PER-31 is fixed. Every process buffers
    # unboundedly for as long as blob writes cannot land, and at high
    # complexity one 45s cut already grows clusterd by GiBs, so repeated cuts
    # OOM-kill it before the run finishes. 15s still exercises the heal edges,
    # which is where the recovery bugs are. Reproducer:
    # bin/mzcompose --find invariants run default --scenario=repro-blob-memory
    "blob": Leg(
        "blob",
        (Proxy("blob", 9000, "minio:9000"),),
        max_outage=15.0,
        # NOTE: limit_data is dropped here, not for the buffering reason above
        # but because max_outage only caps the full-outage kinds. A limit_data
        # on this leg kills every connection past its byte budget, so it stalls
        # blob writes for its whole uncapped duration, which is all PER-31
        # needs: nightly 17743 OOM-killed clusterd at 6.2GiB during a 29s one.
        kinds=("disable", "timeout", "reset_peer", "flap"),
    ),
    # One per clusterd, see PUBSUB_PORTS. Losing pubsub must cost latency and
    # nothing else: it is a notification channel, and every reader that stops
    # hearing from it falls back to polling. A correctness failure here is a
    # real one, and a partial cut is the only way to reach the state where one
    # process is behind and the others are not.
    **{
        f"pubsub-{name.removeprefix('clusterd-')}": Leg(
            f"pubsub-{name.removeprefix('clusterd-')}",
            (Proxy(f"pubsub_{name.replace('-', '_')}", port, "materialized:6879"),),
        )
        for name, port in PUBSUB_PORTS.items()
    },
    "pg": Leg("pg", (Proxy("pg", 5432, "postgres:5432"),)),
    "mysql": Leg("mysql", (Proxy("mysql", 3306, "mysql:3306"),)),
    "sql-server": Leg("sql-server", (Proxy("sql_server", 1433, "sql-server:1433"),)),
    "kafka-source": Leg("kafka-source", (Proxy("kafka_source", 9092, "kafka:9092"),)),
    "kafka-sink": Leg("kafka-sink", (Proxy("kafka_sink", 9192, "kafka:9192"),)),
    "csr": Leg("csr", (Proxy("csr", 8081, "schema-registry:8081"),)),
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
        nargs="?",
        const="latest",
        default=None,
        metavar="IMAGE",
        help="start Materialize on this image and swap to the current build"
        " mid-chaos, an upgrade under load and disruptions. Bare or 'latest'"
        " resolves to the most recent published release",
    )
    parser.add_argument(
        "--no-disruptions",
        action="store_true",
        help="run the workload and checkers without any disruptions",
    )
    parser.add_argument(
        "--legs",
        type=str,
        default=None,
        metavar="NAME,...",
        help="disrupt only these legs of the scenario's set, for bisecting"
        " which dependency a symptom depends on",
    )
    parser.add_argument(
        "--kinds",
        type=str,
        default=None,
        metavar="KIND,...",
        help="use only these disruption kinds, both toxics and process"
        " faults, for exercising one kind or bisecting which one a symptom"
        " depends on. A leg or process left with no allowed kind is skipped",
    )
    parser.add_argument(
        "--persist-churn",
        action="store_true",
        help="drive persist compaction, rollups, and GC as hard as the"
        " workload allows, see PERSIST_CHURN_SQL",
    )
    args = parser.parse_args()

    if args.upgrade_from == "latest":
        args.upgrade_from = f"materialize/materialized:{get_latest_published_version()}"
        print(f"--- Upgrading from {args.upgrade_from}")
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


class MemorySampler(threading.Thread):
    """Records each container's resident memory into the event log.

    A process that grows without bound is only visible here as a kernel OOM
    line in a log nobody reads, after the fact, with no way to tell which
    disruption drove the growth or how fast it went. The disruptor writes its
    history to the same log, so sampling into it makes the two line up.

    Growth past a fraction of the container's limit is echoed as well, so it
    shows up in the job output while the process is still alive rather than
    only in the artifact afterwards, and a clusterd that gets there has a heap
    profile taken. A kernel OOM kill leaves no panic, no core, and no trace of
    where the memory went, so the profile has to be taken while the process is
    still alive or the evidence is gone for good.
    """

    ECHO_FRACTION = 0.7
    # Far enough below ECHO_FRACTION that the profile is taken while the
    # process still has room to answer the request, and above any settled
    # working set we have measured.
    PROFILE_FRACTION = 0.5
    INTERVAL = 15.0

    def __init__(self, c: Composition, log: EventLog) -> None:
        super().__init__(name="memory-sampler", daemon=True)
        self.c = c
        self.log = log
        self.stop_event = threading.Event()
        self.peak: dict[str, float] = {}
        self.profiled: set[str] = set()

    def run(self) -> None:
        while not self.stop_event.wait(self.INTERVAL):
            try:
                out = subprocess.run(
                    [
                        "docker",
                        "stats",
                        "--no-stream",
                        "--format",
                        "{{.Name}}\t{{.MemUsage}}\t{{.MemPerc}}",
                    ],
                    check=True,
                    capture_output=True,
                    text=True,
                    timeout=30,
                ).stdout
            except Exception:
                # Sampling is diagnostics. Docker being briefly unavailable
                # (a container mid-restart, the daemon busy) must never
                # disturb the run.
                continue
            rows = []
            for line in out.splitlines():
                parts = line.split("\t")
                if len(parts) != 3 or "invariants-" not in parts[0]:
                    continue
                short = parts[0].removeprefix("invariants-").removesuffix("-1")
                rows.append(f"{short}={parts[1].split(' / ')[0]}")
                try:
                    pct = float(parts[2].rstrip("%"))
                except ValueError:
                    continue
                if (
                    pct >= self.PROFILE_FRACTION * 100
                    and short.startswith("clusterd-")
                    and short not in self.profiled
                ):
                    self.profiled.add(short)
                    _heap_profile(self.c, short, f"{pct:.0f}pct", self.log)
                if pct >= self.ECHO_FRACTION * 100 and pct > self.peak.get(short, 0.0):
                    self.peak[short] = pct
                    self.log.log("mem", f"{short} at {pct:.0f}% of its memory limit")
            if rows:
                self.log.log("mem", " ".join(rows), echo=False)


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

    with ExitStack() as stack:
        # restart on-failure: proxies live in toxiproxy's memory, so after a
        # crash the disruptor's heal re-creates them (it cannot resurrect the
        # container itself).
        stack.enter_context(
            c.override(Toxiproxy(seed=rng.randrange(2**63), restart="on-failure"))
        )
        # The old-version services of an --upgrade-from run live in their own
        # stack: up() renders the current composition, so the mid-run upgrade
        # swap must be able to close this override (restoring the
        # current-build definitions) without disturbing the toxiproxy one.
        version_override = stack.enter_context(ExitStack())
        if args.upgrade_from:
            version_override.enter_context(
                c.override(
                    materialized_service(image=args.upgrade_from),
                    *(
                        clusterd_service(name, image=args.upgrade_from)
                        for name in CLUSTERD_NAMES
                    ),
                )
            )
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
        if args.persist_churn:
            c.sql(PERSIST_CHURN_SQL, port=6877, user="mz_system")

        endpoints = Endpoints(
            mz_host="127.0.0.1",
            mz_port=c.default_port("materialized"),
            mz_system_port=c.port("materialized", 6877),
            mz_http_port=c.port("materialized", 6876),
            minio_port=c.default_port("minio"),
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
        kind_filter = None
        if args.kinds:
            kind_filter = {k.strip() for k in args.kinds.split(",") if k.strip()}
            unknown = kind_filter - set(DISRUPTION_KINDS) - PROCESS_KINDS
            assert not unknown, f"--kinds: unknown: {sorted(unknown)}"
        if args.legs:
            selected = set(args.legs.split(","))
            unknown = selected - {leg.name for leg in legs}
            assert not unknown, f"--legs: not in this scenario: {sorted(unknown)}"
            legs = [leg for leg in legs if leg.name in selected]
        midrun = (
            make_upgrade_swap(c, ctx, version_override) if args.upgrade_from else None
        )
        sampler = MemorySampler(c, log)
        sampler.start()
        try:
            runner = Runner(
                scenario,
                args.runtime,
                toxiproxy,
                legs,
                process_targets(c),
                midrun_event=midrun,
                restore_proxies=[
                    proxy for leg in LEGS.values() for proxy in leg.proxies
                ],
                restart_toxiproxy=lambda: _restart_toxiproxy(c, toxiproxy),
                kind_filter=kind_filter,
            )
            try:
                runner.run()
            except BaseException:
                # A failure is when the shard history is most worth having,
                # not least: what a run reports is often a symptom (a wedged
                # checker, a cancelled query) of a persist event that left no
                # other trace, and the state history is the only place that
                # event is still visible. Diagnostics only here, so nothing
                # can mask or replace the failure being raised.
                try:
                    audit_persist_history(c, ctx, log)
                except Exception as e:
                    log.log("audit", f"post-failure state audit did not run: {e}")
                raise
            # After the run, so it sees the whole history the run produced.
            try:
                audit_persist_history(c, ctx, log)
            except InvariantViolation:
                raise
            except Exception as e:
                # The audit is a second opinion on a run whose own invariants
                # already held. Its plumbing (a sidecar container, a CLI, a
                # catalog query) failing is not a verdict on the run.
                log.log("audit", f"persist state audit did not run: {e}")
        finally:
            sampler.stop_event.set()
            scenario.teardown()


def make_upgrade_swap(
    c: Composition, ctx: ScenarioContext, version_override: ExitStack
):
    # Captured now, while the old version is still healthy: mid-chaos it may
    # not answer, and the post-swap probe needs it to prove the version
    # actually changed.
    client = MzClient(ctx, "upgrade-swap")
    old_version = str(client.query("SELECT mz_version()")[0][0])
    client.reset()

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

        unpause_quietly(c, *names)
        # The disruptor may kill one of these at any moment, and a
        # `docker compose kill` naming a container that is not running fails
        # the whole invocation. Kill what is running, and retry once the set
        # has been re-observed if the disruptor won the race in between. A
        # container the disruptor killed is already in the state the swap
        # needs, and up() recreates it on the new image either way, so the
        # guarantee that no process survives on the old image still holds.
        for attempt in range(3):
            running = [name for name in names if c.is_running(name)]
            if not running:
                break
            try:
                c.kill(*running)
                break
            except Exception:
                if attempt == 2:
                    raise
        # up() renders the composition, which the version override pins to
        # the old image, so the override must be closed first.
        version_override.close()
        # A pause can also land between the kill and the up, so retry around
        # unpausing rather than relying on up()'s own retries, which would
        # hit the same paused container three times.
        for attempt in range(3):
            try:
                c.up(*names, detach=True)
                break
            except Exception:
                if attempt == 2:
                    raise
                unpause_quietly(c, *names)

        def upgraded() -> bool:
            version = str(client.query("SELECT mz_version()", timeout=30)[0][0])
            if version == old_version:
                raise InvariantViolation(
                    f"upgrade swap brought back the old version {version}"
                )
            return True

        # Bounded probe rather than one-shot: legs may still be disrupted
        # when the swap fires, but a full metadata cut is capped at 45s and
        # the new build must serve after healing.
        wait_until(upgraded, 180, "swapped-in build serving its new version")

    return swap


# Caps on the post-run persist audit: overall, per shard, per shard's output,
# and on the size of shard it looks at in the first place.
AUDIT_BUDGET_S = 240.0
AUDIT_SHARD_TIMEOUT_S = 20
AUDIT_MAX_BYTES = 50_000_000
AUDIT_MAX_PARTS = 500


def audit_persist_history(c: Composition, ctx: ScenarioContext, log: EventLog) -> None:
    """Fails the run if a shard's live state history resurrects a blob.

    Every blob key a shard references must be live over one contiguous run of
    seqnos: it enters state when a batch referencing it is added and leaves
    when the last one is removed, and nothing may bring it back. A key that is
    live, gone, and live again means a state transition was applied to a state
    it was not computed from, which is how a stale merge res or a replayed
    rollup insert corrupts a shard.

    Persist notices this itself only in narrow circumstances: GC's part check
    (`gc.rs`) needs the key to land in the same delete batch it is walking, and
    `ReferencedBlobValidator` is compiled out of the profiles that build our
    images. This audit sees every resurrection in the live window regardless
    of whether GC happened to run, so it is worth the one pass per shard.
    """
    from materialize.mzcompose.composition import Service as ServiceName

    c.up(ServiceName("persistcli", idle=True))

    def inspect(*args: str) -> str:
        return c.exec(
            "persistcli",
            "persistcli",
            "inspect",
            *args,
            f"--blob-uri={minio_blob_uri()}",
            capture=True,
            silent=True,
        ).stdout

    # Enumerated from blob rather than from mz_storage_shards, which knows
    # only about storage objects, so this also covers the catalog and txn-wal
    # shards. Big shards are skipped: `state-diff` prints the whole state
    # twice per transition, which for the bank tables is gigabytes. Those are
    # also the shards GC walks constantly under --persist-churn, so its own
    # part check covers them. What it does not cover is a shard that is
    # dropped before GC ever gets to it, which is exactly what is left here.
    counts = json.loads(inspect("blob-count"))
    shards = sorted(
        (
            shard
            for shard, count in counts.items()
            if count["batch_part_count"] <= AUDIT_MAX_PARTS
        ),
        key=lambda shard: -counts[shard]["batch_part_count"],
    )
    # Keys are "<writer>/<part>", and only their identity matters here.
    key_re = re.compile(r'"([a-z0-9]+/[a-z][0-9a-f-]{36})"')
    findings = []
    audited = 0
    truncated = 0
    # An environment has more shards than a post-run check should spend
    # minutes on, and the ones a run churned are as good a sample as any.
    deadline = time.monotonic() + AUDIT_BUDGET_S
    for shard in shards:
        if time.monotonic() > deadline:
            break
        try:
            # Bounded in both time and bytes: state size times version count
            # has no useful upper bound, and this runs inside a test.
            out = c.exec(
                "persistcli",
                "bash",
                "-c",
                f"timeout {AUDIT_SHARD_TIMEOUT_S} persistcli inspect state-diff"
                f" --shard-id={shard}"
                " --consensus-uri=postgres://root@postgres-metadata:26257"
                "?options=--search_path=consensus"
                f" --blob-uri='{minio_blob_uri()}' 2>/dev/null"
                f" | head -c {AUDIT_MAX_BYTES}",
                capture=True,
                silent=True,
            ).stdout
        except Exception as e:
            # A shard finalized between the query and here, or a persistcli
            # that cannot reach its stores. Neither says anything about the
            # invariant, and the audit is not the run's purpose.
            log.log("audit", f"{shard}: inspect failed ({e})", echo=False)
            continue
        # One JSON object per line, each holding the states before and after
        # one transition, oldest first.
        states = []
        for line in out.splitlines():
            if not line.strip():
                continue
            try:
                transition = json.loads(line)
            except json.JSONDecodeError:
                # The byte cap cuts the last line in half. Everything before
                # it is still a valid prefix of the history, so the shard is
                # audited, just not to its end.
                truncated += 1
                break
            if not states:
                states.append(transition["previous"])
            states.append(transition["new"])
        live_at: dict[str, list[int]] = {}
        for index, state in enumerate(states):
            for key in set(key_re.findall(json.dumps(state))):
                live_at.setdefault(key, []).append(index)
        for key, indexes in live_at.items():
            if indexes != list(range(indexes[0], indexes[-1] + 1)):
                seqnos = [states[i].get("seqno") for i in indexes]
                findings.append(f"{shard} resurrects {key}, live at seqnos {seqnos}")
        audited += 1
    if findings:
        raise InvariantViolation("persist state history: " + "; ".join(findings[:5]))
    log.log(
        "audit",
        f"no resurrected blobs across {audited}/{len(shards)} shards"
        f" ({truncated} truncated at the byte cap)",
    )


# Makes persist take as many state transitions per second as it can: small
# batches so every write compacts, compaction claimable by any process so two
# of them race on the same spine range, and rollups often enough that GC has
# something to truncate on every pass. The disruptions then have something to
# interleave with, which is what turns a lost consensus response into a
# retried non-idempotent state transition.
PERSIST_CHURN_SQL = """
ALTER SYSTEM SET persist_inline_writes_single_max_bytes = 0;
ALTER SYSTEM SET persist_inline_writes_total_max_bytes = 0;
ALTER SYSTEM SET persist_blob_target_size = 4096;
ALTER SYSTEM SET persist_compaction_heuristic_min_inputs = 2;
ALTER SYSTEM SET persist_compaction_heuristic_min_parts = 1;
ALTER SYSTEM SET persist_compaction_heuristic_min_updates = 1;
ALTER SYSTEM SET persist_claim_unclaimed_compactions = true;
ALTER SYSTEM SET persist_claim_compaction_percent = 100;
-- A partial replacement keeps the spine id of the batch it rewrites, which is
-- the one state change that leaves a previously applied merge res still
-- matching the range it was computed for. Short runs make partial
-- replacements the common case. (The agitator flips both back and forth, so
-- these are starting points, not pins.)
ALTER SYSTEM SET persist_enable_incremental_compaction = true;
ALTER SYSTEM SET persist_batch_max_run_len = 2;
ALTER SYSTEM SET persist_rollup_threshold = 8;
ALTER SYSTEM SET persist_gc_min_versions = 8;
"""


def unpause_quietly(c: Composition, *names: str) -> None:
    """Unpause containers, ignoring those that were not paused.

    A paused container can be neither killed nor started: docker refuses a
    start with "cannot start a paused container", and a kill of a paused
    container is not reliable either. Everything in the harness that kills or
    starts a container races the disruptor's pause of the same container, so
    they all unpause first. Unpausing a running container is an error rather
    than a no-op, which is why the result is discarded.
    """
    for name in names:
        try:
            c.unpause(name)
        except Exception:
            pass


def process_targets(c: Composition) -> list[ProcessTarget]:
    """Processes the disruptor may kill or pause.

    The clusterd containers restart automatically (restart on-failure), so
    their heal is an idempotent up(). Upstream databases are deliberately
    absent: they run with fsync disabled, so a SIGKILL could lose committed
    data and invalidate the oracle.
    """

    def target(name: str, max_outage: float = 120.0) -> ProcessTarget:
        def kill() -> None:
            unpause_quietly(c, name)
            c.kill(name)

        def heal() -> None:
            unpause_quietly(c, name)
            # Bounded retries: an up that cannot succeed (e.g. dead
            # proxies) must not wedge the disruptor for minutes, but a
            # single attempt can race a crash-looping restart policy.
            c.up(name, detach=True, max_tries=3)

        def update_cpu(*args: str) -> str:
            # `docker update`, not compose: changing the CPU allowance of a
            # running container in place is the only way to degrade a process
            # without restarting it, and compose has no equivalent. A
            # container the disruptor killed moments ago is simply gone, and
            # the caller treats a failed disruption as a lost cycle.
            #
            # The quota/period pair rather than `--cpus`, because only the
            # quota can be cleared again: `--cpus=0` is read as "leave the
            # limit alone", so a heal written that way silently does nothing
            # and the process stays crippled for the rest of the run. The
            # two also disagree in `docker inspect`, since setting `--cpus`
            # reports through NanoCpus and leaves CpuQuota at 0.
            container = f"{c.project_name}-{name}-1"
            subprocess.run(
                ["docker", "update", *args, container],
                check=True,
                capture_output=True,
            )
            quota = subprocess.run(
                ["docker", "inspect", "-f", "{{.HostConfig.CpuQuota}}", container],
                check=True,
                capture_output=True,
                text=True,
            )
            return quota.stdout.strip()

        def throttle() -> None:
            update_cpu(f"--cpu-period={CPU_PERIOD}", f"--cpu-quota={CPU_THROTTLED}")

        def unthrottle() -> None:
            # Verified and retried, not assumed. A heal that quietly fails to
            # restore the allowance turns one disruption into a permanent one,
            # which is invisible except as everything downstream slowly
            # falling apart. Retried because a container killed by another
            # disruption cannot be updated while it is down, and a restart
            # policy brings back the same container with the same limit.
            last = ""
            for attempt in range(3):
                try:
                    last = update_cpu("--cpu-quota=-1")
                    if last == "-1":
                        return
                except Exception as e:
                    last = str(e)
                if attempt < 2:
                    time.sleep(2.0)
            raise RuntimeError(f"{name} still CPU limited after heal: {last}")

        return ProcessTarget(
            name=name,
            max_outage=max_outage,
            kill=kill,
            heal=heal,
            pause=lambda: c.pause(name),
            unpause=lambda: c.unpause(name),
            # Half a core: a process that is merely slow is the case every
            # timeout in the system is implicitly betting against, and unlike
            # a pause it keeps answering, so peers see a live but lagging peer
            # rather than an absent one. Squeezing harder than this makes the
            # coordinator unable to drain its backlog at all, which is a plain
            # outage with extra steps and is what `pause` is for.
            throttle=throttle,
            unthrottle=unthrottle,
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
    "repro-blob-memory": "PER-31: unbounded clusterd memory while the blob"
    " store is cut",
    "repro-storage-memory": "SS-428: unbounded clusterd-storage memory"
    " replaying a source backlog while a new table's rewind pins the frontier",
    "repro-postheal-stall": "PER-32: writes stalled long after a metadata cut"
    " healed",
    "repro-per10": "PER-10: persist GC panic, earliest state without rollup",
    "repro-durable-resume": "resumed SUBSCRIBE cancels its carried state",
    "repro-compute-asof": "PER-49: compute halts hydrating a dataflow past its"
    " as_of",
    "repro-replacement-brick": "SQL-603: bootstrap halts forever on an"
    " interrupted replacement apply",
    "repro-stale-merge-res": "a merge res retried after a lost consensus"
    " response overwrites a newer compaction",
    "repro-alter-table-schema": "SQL-616 / PER-59: a kill inside ALTER TABLE"
    " ADD COLUMN leaves the catalog and the shard's schema out of step",
}


# Background load for a repro, as (scenario, actions). The first action runs
# every tick for steady write pressure, the rest rotate slowly because they are
# DDL. TableBank's ledger transfers keep consensus busy and its created and
# dropped views mean shard finalization, which keeps persist GC busy, so a
# finding only needs its own entry when it lives in another scenario's shape.
DEFAULT_REPRO_LOAD = ("table-bank", ("ledger-transfer", "ddl-churn"))
REPRO_LOAD = {
    # The source-table churn is driven by the repro itself, at the moment in
    # the sequence that matters, not at random.
    "repro-storage-memory": ("pg-cdc-bank", ("cdc-transfer", "wide-txn")),
}


def run_repro(c: Composition, name: str, args, log: EventLog) -> None:
    scenario_name, load_actions = REPRO_LOAD.get(name, DEFAULT_REPRO_LOAD)
    scenario_class = SCENARIOS[scenario_name]

    c.down(destroy_volumes=True)
    rng = random.Random(f"{args.seed}-{name}")
    with c.override(Toxiproxy(seed=rng.randrange(2**63), restart="on-failure")):
        c.up("toxiproxy")
        toxiproxy = ToxiproxyApi(f"http://127.0.0.1:{c.default_port('toxiproxy')}")
        for leg in LEGS.values():
            for proxy in leg.proxies:
                toxiproxy.create(proxy)
        c.up(
            "materialized",
            "clusterd-compute",
            "clusterd-compute2",
            "clusterd-storage",
            *scenario_class.services,
        )
        c.sql(CLUSTER_SETUP_SQL, port=6877, user="mz_system")
        ctx = ScenarioContext(
            endpoints=Endpoints(
                mz_host="127.0.0.1",
                mz_port=c.default_port("materialized"),
                mz_system_port=c.port("materialized", 6877),
                pg_port=(
                    c.default_port("postgres")
                    if "postgres" in scenario_class.services
                    else None
                ),
            ),
            complexity=COMPLEXITIES[args.complexity],
            rng=rng,
            log=log,
            seed=args.seed,
        )
        scenario = scenario_class(ctx)
        scenario.setup()
        bundles = [
            scenario.make_worker(i, random.Random(rng.randrange(2**31)))
            for i in range(4)
        ]
        stop = threading.Event()

        def drive(bundle) -> None:
            actions = {action.name: action for action in bundle.actions}
            steady, *periodic = load_actions
            ticks = 0
            while not stop.is_set():
                ticks += 1
                if periodic and ticks % 40 == 0:
                    action = actions[periodic[(ticks // 40) % len(periodic)]]
                else:
                    action = actions[steady]
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


def _restart_toxiproxy(c: Composition, api: ToxiproxyApi) -> None:
    """Replace a toxiproxy whose admin API no longer answers.

    Every leg runs through this container, so the restart cuts them all for a
    moment. That is the same event as a toxiproxy crash, which the disruptor
    recovers from by re-creating the proxies, and it is the only way out of a
    wedged admin API.

    The admin port is published on an ephemeral host port, which a recreated
    container does not keep, so the API is re-pointed at the new mapping.
    """
    c.kill("toxiproxy")
    c.up("toxiproxy", detach=True, max_tries=3)
    api.rebind(f"http://127.0.0.1:{c.default_port('toxiproxy')}")


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


def _heap_profile(c: Composition, service: str, label: str, log: EventLog) -> None:
    """Save a symbolized heap profile of `service` next to the event log.

    Our images build jemalloc with profiling active, and clusterd's internal
    HTTP server serves the dump, so nothing has to be arranged in advance. The
    result is a folded-stack `.mzfg`, which both the flamegraph viewer and
    `sort`/`grep` can read, so a run that grew can be attributed from the
    artifacts alone.
    """
    path = f"heap-{service}-{label}.mzfg"
    try:
        port = c.port(service, 6878)
        with open(path, "wb") as f:
            subprocess.run(
                [
                    "curl",
                    "-sS",
                    "--max-time",
                    "120",
                    "-X",
                    "POST",
                    "-d",
                    "action=dump_sym_mzfg",
                    f"http://127.0.0.1:{port}/",
                ],
                check=True,
                stdout=f,
                timeout=180,
            )
        log.log("repro", f"heap profile of {service} written to {path}")
    except Exception as e:
        log.log("repro", f"heap profile of {service} ({label}) failed: {e}")


def repro_storage_memory(c, toxiproxy, ctx, log) -> None:
    """SS-428: add a table to a Postgres source that has a replication backlog.

    Adding a table to a running source makes the snapshot operator emit a
    rewind request for the new export, and the replication reader then holds
    its data capability at offset zero until it has replayed past the new
    table's snapshot LSN, because that is where the rewind's negated updates
    go. The source's frontier therefore does not move for the whole replay.

    A snapshot on its own survives that: all of its rows carry offset zero, so
    they land in one persist batch builder, which spills parts to blob once it
    passes `persist_blob_target_size`. The replay does not. Its rows carry
    real timestamps, one per remap binding, so they land in one open builder
    per second of replay, and none of them can be finished while the frontier
    is pinned. Everything replayed stays resident until the rewind clears.

    So the growth is the size of the backlog, and it does not stop until the
    replay has caught up. This builds the backlog with a source-leg outage and
    bulk upstream writes, then adds the table the moment the leg is whole
    again. The comparison is the same sequence without the added table: that
    replay commits as it goes and stays flat.

    While it grows, `SELECT` on any of the source's tables blocks, which is
    the same pinned frontier seen from the outside.
    """
    container = "invariants-clusterd-storage-1"
    # The upstream keeps committing through this, so it sets the size of the
    # replay. Also stops the slot's confirmed_flush_lsn from advancing, which
    # is where the replay starts from.
    OUTAGE_S = 150.0
    # Rows per bulk statement. Server-side generate_series, so the harness is
    # not the bottleneck and the outage is worth GiBs of WAL.
    BULK_ROWS = 100_000
    # Growth this far above the settled baseline is staged replay, not a
    # replay working through: the same outage without the added table peaks
    # 0.15GiB above its baseline and drains within a minute. Measured growth
    # once the rewind is pending is 30-60MiB/min and does not stop, so the
    # watch has to be long enough for the slow end of that to clear the
    # threshold on a loaded agent.
    GROWTH_GIB = 0.75
    WATCH_S = 2400.0

    import psycopg

    client = MzClient(ctx, "repro-source-table")
    stop_bulk = threading.Event()

    def connect_upstream():
        # Direct, not through the proxy: the upstream has to keep committing
        # while the source's leg is cut.
        return psycopg.connect(
            host=ctx.endpoints.mz_host,
            port=ctx.endpoints.pg_port,
            user="postgres",
            password="postgres",
            dbname="postgres",
            connect_timeout=10,
        )

    def bulk_writer(idx: int) -> None:
        conn: Any = None
        seq = 0
        while not stop_bulk.is_set():
            seq += 1
            try:
                if conn is None:
                    conn = connect_upstream()
                # Self-consistent rows, so the wide-transaction invariant
                # still holds if this scenario is ever checked here.
                conn.cursor().execute(
                    "INSERT INTO wide_txn (txn_id, txn_rows, idx) SELECT"
                    f" {900_000_000 + idx * 1_000_000 + seq}, {BULK_ROWS}, g"
                    f" FROM generate_series(1, {BULK_ROWS}) g"
                )
                conn.commit()
            except Exception:
                if conn is not None:
                    try:
                        conn.close()
                    except Exception:
                        pass
                    conn = None
                stop_bulk.wait(1.0)

    writers = [
        threading.Thread(target=bulk_writer, args=(i,), daemon=True) for i in range(4)
    ]
    try:
        time.sleep(30)
        base = min(_rss_gb(container) for _ in range(3))
        log.log("repro", f"baseline {base:.2f}GiB")
        _heap_profile(c, "clusterd-storage", "baseline", log)

        # Only while the leg is cut: the backlog should be this window's
        # writes and nothing more, and the upstream should stop growing once
        # the replay it feeds has started.
        toxiproxy.set_enabled("pg", False)
        log.log("repro", f"source leg cut for {OUTAGE_S:.0f}s, building a backlog")
        for t in writers:
            t.start()
        try:
            time.sleep(OUTAGE_S)
        finally:
            stop_bulk.set()
            for t in writers:
                t.join(timeout=30)
            _toxiproxy_heal(toxiproxy, log)

        # Immediately, while the backlog is still unreplayed: the rewind has
        # to be pending for the replay, not after it.
        deadline = time.monotonic() + 60
        while True:
            try:
                client.write(
                    "CREATE TABLE accounts_stuck FROM SOURCE bank_source"
                    " (REFERENCE accounts)"
                )
                break
            except Exception as e:
                if time.monotonic() > deadline:
                    raise AssertionError(
                        f"could not add a table to the source: {e}"
                    ) from e
                time.sleep(0.5)
        log.log("repro", "table added while the backlog replays, watching memory")

        peak = base
        started = time.monotonic()
        deadline = started + WATCH_S
        # The replay takes tens of minutes to clear the threshold, so the
        # watch reports as it goes. A loop this long that prints only its
        # verdict is indistinguishable from a hang.
        next_report = started
        while time.monotonic() < deadline:
            time.sleep(5)
            now = _rss_gb(container)
            peak = max(peak, now)
            if time.monotonic() >= next_report:
                next_report = time.monotonic() + 30
                log.log(
                    "repro",
                    f"{time.monotonic() - started:.0f}s into the replay:"
                    f" clusterd-storage {now:.2f}GiB, {now - base:+.2f}GiB from"
                    f" baseline, threshold {base + GROWTH_GIB:.2f}GiB",
                )
            if now > base + GROWTH_GIB:
                _heap_profile(c, "clusterd-storage", "grown", log)
                raise AssertionError(
                    f"REPRODUCED: clusterd-storage grew {base:.2f} ->"
                    f" {now:.2f}GiB staging a replay it cannot commit while a"
                    " newly added table's rewind pins the source frontier"
                )
        log.log(
            "repro", f"not reproduced, peaked at {peak:.2f}GiB (base {base:.2f}GiB)"
        )
    finally:
        stop_bulk.set()
        for t in writers:
            if t.is_alive():
                t.join(timeout=10)


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
                    f"INSERT INTO ledger (worker, seq, account, amount)"
                    f" VALUES ({1000 + idx}, {seq}, 0, 0)",
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
    """Blob cut that freezes writes, envd death, then a durable resume.

    The observed occurrence had two ingredients: a blob cut that froze every
    write for 89s, so the subscriber's resume timestamp fell far behind the
    shard, and envd dying seconds after the heal from the PER-10 GC panic. A
    scripted clean kill and restart on its own was ruled out as a trigger, so
    each cycle first gives that panic a window to fire (envd's restart policy
    brings it back) and kills only when it does not, recording which trigger
    the cycle actually got. Cut lengths alternate between the incident's long
    write freeze and repro-per10's short cut, whose heal is where the panic
    fires.
    """
    from materialize.invariants.checkers import SubscribeChecker
    from materialize.invariants.framework import InvariantViolation

    # Long cuts match the incident's write freeze, short ones repro-per10.
    CUT_LENGTHS = [90.0, 5.0, 90.0, 5.0, 90.0]
    # Grace period for the GC panic to kill envd on its own after a heal.
    PANIC_WINDOW_S = 15.0

    def rollup_panics() -> int:
        logs = c.invoke("logs", "materialized", capture=True).stdout or ""
        return logs.count("did not have corresponding rollup")

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

    for iteration, cut_s in enumerate(CUT_LENGTHS):
        rng = random.Random(f"resume-{iteration}")
        checker = ResumeChecker(rng)
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            try:
                checker.check_once()
            except InvariantViolation:
                # No disruption is active yet, a violation here is real.
                raise
            except Exception:
                pass
        assert (
            checker.last_validated_ts is not None
        ), "warmup validated no timestamp, so there is no state to carry"
        resume_from = checker.last_validated_ts
        panics_before = rollup_panics()
        toxiproxy.set_enabled("blob", False)
        time.sleep(cut_s)
        _toxiproxy_heal(toxiproxy, log)
        trigger = "clean kill (ruled out on its own)"
        panic_deadline = time.monotonic() + PANIC_WINDOW_S
        while time.monotonic() < panic_deadline:
            if rollup_panics() > panics_before:
                trigger = "PER-10 panic"
                break
            time.sleep(1)
        if trigger != "PER-10 panic":
            c.kill("materialized")
        c.up("materialized", detach=True, max_tries=3)
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            try:
                checker.check_once()  # raises InvariantViolation if reproduced
            except AssertionError as e:
                # Carry the violation through: it holds the shard-vs-stream
                # probe and the session context that classify the occurrence.
                raise AssertionError(
                    f"REPRODUCED durable-resume anomaly, iteration"
                    f" {iteration} ({trigger}): {e}"
                ) from e
            except Exception:
                pass
        # resumes==0 means the resume never ran, which is not evidence of
        # consistency, so the count is part of the result.
        log.log(
            "repro",
            f"iteration {iteration}: {cut_s:.0f}s cut, {trigger}, resumed"
            f" {checker.resumes}x from {resume_from}, stayed consistent",
        )
        checker.close()
    log.log("repro", f"not reproduced in {len(CUT_LENGTHS)} iterations")


def repro_compute_asof(c, toxiproxy, ctx, log) -> None:
    """Compute halts hydrating a dataflow whose input compacted past its as_of.

    The halt is tracked as PER-49, the retry mechanism that would absorb it as
    CPU-34, and it was seen in incidents-and-escalations #39. When the
    controller loses a
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


def repro_replacement_brick(c, toxiproxy, ctx, log) -> None:
    """Kill environmentd while a replacement is applied, then fail to boot.

    The storage controller's bootstrap halts whenever a dependency read hold
    has an empty since, calling it a concurrent deletion. The condition it
    tests is narrower than the one its message claims: it does not check the
    dependent's own write frontier, and the check right below it documents why
    that matters ("We don't care about the dependency since when the write
    frontier is empty. In that case, no-one can write down any more updates.").

    A replacement MV holds a read hold on the collection it replaces, so an
    apply that is interrupted can leave the replacement registered while the
    replaced collection is already finalized. Both frontiers are then empty,
    which is benign, and bootstrap halts on it anyway. The state is durable, so
    every restart halts again and environmentd never comes back:
    src/storage-controller/src/lib.rs:992, nightly 17740 spent 139 boots there.

    SQL-603. NOTE: this presses the sequence but has not reproduced it, in 30
    kill-during-apply cycles across two variants (the second delays the
    metadata leg to widen the window between the storage finalize and the
    catalog commit). The nightly hit it in 6 of 8 upgrade runs, so an
    ingredient here is still missing, most likely the concurrent load and the
    other dependents of `total` that a full scenario has.
    """
    from materialize.invariants.mz import MzClient
    from materialize.invariants.scenarios.table_bank import TOTAL_DEF

    MARKER = "dependency since frontier is empty"

    def bricked() -> bool:
        logs = c.invoke("logs", "materialized", capture=True).stdout or ""
        return logs.count(MARKER) > 0

    applier = MzClient(ctx, "brick-applier")
    driver = MzClient(ctx, "brick-driver")
    for iteration in range(15):
        driver.write("DROP MATERIALIZED VIEW IF EXISTS total_repl")
        driver.write(
            "CREATE REPLACEMENT MATERIALIZED VIEW total_repl FOR total"
            " IN CLUSTER compute WITH (RETAIN HISTORY = FOR '600s')"
            f" AS {TOTAL_DEF}"
        )
        # Applying an unhydrated replacement is rejected, so wait for it as the
        # documented workflow does.
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            rows = driver.query(
                "SELECT bool_and(h.hydrated)"
                " FROM mz_internal.mz_hydration_statuses h"
                " JOIN mz_catalog.mz_materialized_views v ON h.object_id = v.id"
                " WHERE v.name = 'total_repl'"
            )
            if rows and rows[0][0]:
                break
            time.sleep(1)

        # The kill has to land inside the apply, which finalizes the replaced
        # collection and retires the replacement in one go.
        def apply() -> None:
            try:
                applier.write(
                    "ALTER MATERIALIZED VIEW total APPLY REPLACEMENT total_repl"
                )
            except Exception:
                pass

        # The apply finalizes the replaced collection and retires the
        # replacement, then commits the catalog transaction, and the state that
        # bricks the next boot is the window in between. Both halves are
        # consensus writes, so delaying the metadata leg stretches that window
        # from microseconds to something a kill can land in.
        for stream in ("upstream", "downstream"):
            toxiproxy.add_toxic(
                "metadata",
                f"apply-delay-{stream}",
                "latency",
                {"latency": 1500, "jitter": 500},
                stream=stream,
            )
        thread = threading.Thread(target=apply, daemon=True)
        thread.start()
        time.sleep(0.5 + 0.5 * (iteration % 12))
        c.kill("materialized")
        for stream in ("upstream", "downstream"):
            toxiproxy.delete_toxic("metadata", f"apply-delay-{stream}")
        try:
            c.up("materialized", detach=True, max_tries=2)
        except Exception as e:
            # A bricked environmentd never becomes healthy, which is the
            # symptom, so the log decides rather than this call.
            log.log(
                "repro", f"iteration {iteration}: environmentd did not come up ({e})"
            )
        thread.join(timeout=30)
        if bricked():
            raise AssertionError(
                "REPRODUCED: environmentd halts at bootstrap and cannot come"
                f" back, iteration {iteration}"
            )
        applier.reset()
        driver.reset()
        log.log("repro", f"iteration {iteration}: booted again")
    log.log("repro", "not reproduced in 15 iterations")


def repro_stale_merge_res(c, toxiproxy, ctx, log) -> None:
    """Race a forced compaction against a merge res whose response is lost.

    The organic version of this needs two compactions of the *same* spine id
    to be applied around one indeterminate retry, and nothing in a short run
    produces the second one reliably: claiming an unclaimed compaction needs
    the previous one to be older than the writer lease, which is a hardcoded
    60 minutes. `persistcli admin force-compaction` has no such rule, it
    ignores active compactions entirely, and its own writes go straight to the
    metadata store rather than through toxiproxy. So it can land a competing
    res for the same range while envd's merge res is stuck retrying.

    Each iteration: start a forced compaction, flap the metadata leg so envd's
    own merge res CaS comes back indeterminate and retries against a state the
    forced compaction has moved, heal, then force a GC to walk the diffs.
    Reproduced means envd panicking in gc.rs, or GC deleting a part current
    state still references, which the post-run audit would catch.
    """
    from materialize.invariants.mz import MzClient

    system = MzClient(
        ctx, "stale-merge-res", user="mz_system", port=ctx.endpoints.mz_system_port
    )
    # envd force-compacts the catalog shard in a background task whose fuel and
    # period are dyncfgs ("we're going to gradually turn this on via dyncfgs").
    # Turned up, it produces a merge res for the same spine ids regular
    # compaction is working on, from the same process, over the same proxied
    # consensus connection. That is the racing pair the bug needs, and unlike
    # `persistcli admin force-compaction` it runs with the real codecs.
    system.query("ALTER SYSTEM SET persist_catalog_force_compaction_wait = '1s'")
    system.query("ALTER SYSTEM SET persist_catalog_force_compaction_fuel = 1000000")
    system.query("ALTER SYSTEM SET persist_rollup_threshold = 8")
    system.query("ALTER SYSTEM SET persist_inline_writes_single_max_bytes = 0")
    system.query("ALTER SYSTEM SET persist_inline_writes_total_max_bytes = 0")
    system.query("ALTER SYSTEM SET persist_blob_target_size = 4096")
    log.log("repro", "catalog forced compaction turned up")

    def panicked() -> bool:
        logs = c.invoke("logs", "materialized", capture=True, silent=True).stdout or ""
        return "batch_parts_to_delete" in logs or "should only be appended once" in logs

    for iteration in range(40):
        # Short cuts, so a CaS that already reached consensus loses only its
        # response. A long outage would just fail the call outright.
        for _ in range(24):
            try:
                toxiproxy.set_enabled("metadata", False)
                time.sleep(0.15)
                toxiproxy.set_enabled("metadata", True)
                time.sleep(0.25)
            except Exception:
                break
        _toxiproxy_heal(toxiproxy, log)
        # Let the retries land and GC walk what they produced.
        time.sleep(8)
        if panicked():
            raise AssertionError(
                f"REPRODUCED: stale merge res corrupted a shard, iteration {iteration}"
            )
        retries = (
            c.invoke("logs", "materialized", capture=True, silent=True).stdout or ""
        ).count("merge_res received an indeterminate")
        log.log(
            "repro",
            f"iteration {iteration}: no corruption yet, {retries} merge_res retries",
        )
    log.log("repro", "not reproduced in 40 iterations")


def repro_alter_table_schema(c, toxiproxy, ctx, log) -> None:
    """Kill environmentd between ALTER TABLE's two halves, then fail to boot.

    The catalog transaction commits first and the persist schema evolution
    runs afterwards, as a catalog implication. A crash in between leaves the
    catalog holding a table version whose schema the data shard never
    registered.

    Two panics come out of that state. Re-running the evolution finds persist
    already past the version it expects and soft-panics instead of treating it
    as done (SQL-616). Once it has failed, bootstrap registers the table with
    txn-wal presenting a desc the shard never registered and dies with "schema
    should be registered" (PER-59), on every boot.

    Hitting that gap needs the offset to be measured rather than guessed. A
    latency toxic on the metadata leg stretches every round trip the ALTER
    makes, not only the evolution, so the pre-commit half alone runs for tens
    of seconds. This calibrates the whole window, bisects it for the moment
    the catalog commit lands, and then kills just after it.
    """
    from materialize.invariants.mz import MzClient

    MARKERS = ("schema should be registered", "schema expectation mismatch")

    def panicked() -> str | None:
        logs = c.invoke("logs", "materialized", capture=True, silent=True).stdout or ""
        return next((m for m in MARKERS if m in logs), None)

    driver = MzClient(ctx, "alter-driver")
    system = MzClient(
        ctx, "alter-system", user="mz_system", port=ctx.endpoints.mz_system_port
    )
    system.query("ALTER SYSTEM SET enable_alter_table_add_column = true")

    def with_delay(add: bool) -> None:
        for stream in ("upstream", "downstream"):
            if add:
                toxiproxy.add_toxic(
                    "metadata",
                    f"alter-delay-{stream}",
                    "latency",
                    {"latency": 1500, "jitter": 500},
                    stream=stream,
                )
            else:
                toxiproxy.delete_toxic("metadata", f"alter-delay-{stream}")

    def attempt(table: str, offset: float) -> tuple[bool, str | None]:
        """ALTER, kill at `offset`, restart. Returns (committed, panic)."""
        driver.write(f"CREATE TABLE {table} (a int, b text)")
        driver.write(f"INSERT INTO {table} VALUES (1, 'x')")

        def alter() -> None:
            try:
                MzClient(ctx, f"alter-{table}").write(
                    f"ALTER TABLE {table} ADD COLUMN c int", timeout=300
                )
            except Exception:
                pass

        with_delay(True)
        thread = threading.Thread(target=alter, daemon=True)
        thread.start()
        time.sleep(offset)
        c.kill("materialized")
        with_delay(False)
        try:
            c.up("materialized", detach=True, max_tries=2)
        except Exception as e:
            log.log("repro", f"{table}: did not come up ({e})")
        thread.join(timeout=60)
        marker = panicked()
        driver.reset()
        try:
            committed = bool(
                driver.query(
                    "SELECT 1 FROM mz_catalog.mz_columns c"
                    " JOIN mz_catalog.mz_tables t ON c.id = t.id"
                    f" WHERE t.name = '{table}' AND c.name = 'c'"
                )
            )
        except Exception:
            committed = False
        return committed, marker

    # 1. How long is one uninterrupted ALTER under the delay?
    driver.write("CREATE TABLE alter_calibrate (a int, b text)")
    with_delay(True)
    started = time.monotonic()
    driver.write("ALTER TABLE alter_calibrate ADD COLUMN c int", timeout=300)
    window = time.monotonic() - started
    with_delay(False)
    log.log("repro", f"one ALTER takes {window:.1f}s under the metadata delay")

    # 2. Bisect for the earliest offset whose kill still leaves the catalog
    #    change committed. That is the moment the transaction lands, and the
    #    implication runs in the seconds right after it.
    low, high = 0.0, window
    for step in range(6):
        mid = (low + high) / 2
        committed, marker = attempt(f"alter_bisect_{step}", mid)
        if marker is not None:
            raise AssertionError(f"REPRODUCED {marker!r} while bisecting at {mid:.1f}s")
        log.log("repro", f"bisect {mid:.1f}s: committed={committed}")
        if committed:
            high = mid
        else:
            low = mid
    log.log("repro", f"catalog commit lands at ~{high:.1f}s of {window:.1f}s")

    # 3. Kill just after the commit, walking outwards until the implication
    #    has certainly finished.
    for step, delta in enumerate([0.2, 0.5, 1.0, 1.5, 2.0, 3.0, 4.0, 6.0]):
        committed, marker = attempt(f"alter_exploit_{step}", high + delta)
        if marker is not None:
            raise AssertionError(
                f"REPRODUCED {marker!r} killing {delta:.1f}s after the commit"
            )
        log.log("repro", f"commit+{delta:.1f}s: committed={committed}, booted again")
    log.log("repro", "not reproduced")


REPRO_FUNCS = {
    "repro-blob-memory": repro_blob_memory,
    "repro-storage-memory": repro_storage_memory,
    "repro-postheal-stall": repro_postheal_stall,
    "repro-per10": repro_per10,
    "repro-durable-resume": repro_durable_resume,
    "repro-compute-asof": repro_compute_asof,
    "repro-replacement-brick": repro_replacement_brick,
    "repro-alter-table-schema": repro_alter_table_schema,
    "repro-stale-merge-res": repro_stale_merge_res,
}
