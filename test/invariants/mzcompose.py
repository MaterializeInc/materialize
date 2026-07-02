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

SERVICES = [
    Toxiproxy(),
    PostgresMetadata(),
    Minio(setup_materialize=True),
    Materialized(
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
    ),
    Clusterd(
        name="clusterd-compute",
        # The controller connects through toxiproxy, so the host in request
        # URIs doesn't match this clusterd's hostname.
        environment_extra=["CLUSTERD_GRPC_HOST="],
        # halt! is a designed recovery path for clusterd.
        restart="on-failure",
    ),
    Clusterd(
        name="clusterd-storage",
        environment_extra=["CLUSTERD_GRPC_HOST="],
        restart="on-failure",
    ),
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
CREATE CLUSTER compute REPLICAS (r1 (
    STORAGECTL ADDRESSES ['toxiproxy:2100'],
    STORAGE ADDRESSES ['clusterd-compute:2103'],
    COMPUTECTL ADDRESSES ['toxiproxy:2101'],
    COMPUTE ADDRESSES ['clusterd-compute:2102'],
    WORKERS 1
));
GRANT ALL ON CLUSTER storage TO materialize;
GRANT ALL ON CLUSTER compute TO materialize;
"""


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    parser.add_argument("--scenario", default="all", choices=["all", *SCENARIOS.keys()])
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
        "--no-disruptions",
        action="store_true",
        help="run the workload and checkers without any disruptions",
    )
    args = parser.parse_args()

    print(f"--- Random seed is {args.seed}")
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
    with c.override(Toxiproxy(seed=rng.randrange(2**63), restart="on-failure")):
        # The proxies must exist before materialized boots: its consensus and
        # timestamp-oracle URLs point at toxiproxy.
        c.up("toxiproxy")
        toxiproxy = ToxiproxyApi(f"http://127.0.0.1:{c.default_port('toxiproxy')}")
        for leg in LEGS.values():
            for proxy in leg.proxies:
                toxiproxy.create(proxy)

        services = ["materialized", "clusterd-compute", "clusterd-storage"]
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
        try:
            Runner(scenario, args.runtime, toxiproxy, legs, process_targets(c)).run()
        finally:
            scenario.teardown()


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
            # A single attempt: retrying an up that cannot succeed (e.g.
            # because the proxies it depends on are gone) would wedge the
            # disruptor for minutes. Failing loudly is better.
            heal=lambda: c.up(name, detach=True, max_tries=1),
            pause=lambda: c.pause(name),
            unpause=lambda: c.unpause(name),
        )

    return [
        target("clusterd-compute"),
        target("clusterd-storage"),
        # A paused envd freezes everything like a metadata cut, so its
        # outages are short for the same edge-coverage reason.
        target("materialized", max_outage=45.0),
    ]
