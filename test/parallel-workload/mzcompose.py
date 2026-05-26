# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Runs a randomized parallel workload stressing all parts of Materialize, can
mostly find panics and unexpected errors. See zippy for a sequential randomized
tests which can verify correctness.
"""

import os
import random

from materialize.mzcompose.composition import (
    Composition,
    Service,
    WorkflowArgumentParser,
)
from materialize.mzcompose.service import Service as MzComposeService
from materialize.mzcompose.services.azurite import Azurite
from materialize.mzcompose.services.cockroach import Cockroach
from materialize.mzcompose.services.kafka import Kafka
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.minio import Mc, Minio
from materialize.mzcompose.services.mysql import MySql
from materialize.mzcompose.services.polaris import Polaris, PolarisBootstrap
from materialize.mzcompose.services.postgres import Postgres, PostgresMetadata
from materialize.mzcompose.services.schema_registry import SchemaRegistry
from materialize.mzcompose.services.sql_server import (
    SqlServer,
    setup_sql_server_testing,
)
from materialize.mzcompose.services.testdrive import Testdrive
from materialize.mzcompose.services.toxiproxy import (
    AZURITE_PROXY_NAME,
    CONSENSUS_PROXY_NAME,
    MINIO_PROXY_NAME,
    Toxiproxy,
    setup_blob_toxiproxy,
    setup_consensus_toxiproxy,
    start_disruption_daemon,
    start_slowdown_daemon,
)
from materialize.mzcompose.services.zookeeper import Zookeeper
from materialize.parallel_workload.parallel_workload import parse_common_args, run
from materialize.parallel_workload.settings import (
    ADDITIONAL_SYSTEM_PARAMETER_DEFAULTS,
    Complexity,
    Scenario,
)

# Consensus + blob travel through toxiproxy.
#
# Consensus baseline is small (catalog/persist ops happen by the
# hundred during normal workload; anything bigger easily exceeds
# testdrive/statement timeouts).
#
# Blob baseline is deliberately high (500ms +/- 200ms): each part
# fetch takes the better part of a second, keeping readers in active
# fetch state for longer windows. This biases the failure mode
# toward the BatchFetcher "lost lease?" panic rather than the Listen
# variant: a reader that's mid-fetch when a consensus blackout
# expires its lease is exactly the timing the fetch panic needs.
#
# The actual stress comes from two disruption daemons (below):
# - consensus blackouts (12-30s) reliably outlast the 10s reader
#   lease, so any reader's lease lapses if it can't heartbeat.
# - blob blackouts (20-45s, higher frequency) keep fetches stuck so
#   they overlap consensus blackouts more often.
CONSENSUS_BASELINE_LATENCY_MS = 20
CONSENSUS_BASELINE_JITTER_MS = 10
# Blob is kept *up* (never blacked out) but slow. The combination of
# a high steady baseline + the periodic slowdown daemon keeps readers
# in active fetch state for many seconds at a time. Crucially, blob
# being reachable is required for the BatchFetcher "lost lease?"
# panic: the compactor needs to actually *delete* GC'd parts via
# blob, and the reader's fetch needs to receive a real 404 (not a
# connection error) before the panic fires.
BLOB_BASELINE_LATENCY_MS = 1500
BLOB_BASELINE_JITTER_MS = 500
BLOB_SLOWDOWN_LATENCY_MS = 6000
BLOB_SLOWDOWN_JITTER_MS = 2000

TOXIPROXY_SYSTEM_PARAMS: dict[str, str] = {
    # Shorten the reader lease so a brief consensus blackout from the
    # disruption daemon can actually expire it. The default is 15
    # minutes; we want something close to the lower edge of a
    # disruption duration.
    "persist_reader_lease_duration": "10s",
    # Persist's pubsub immediately pushes every committed state diff
    # to all subscribers - which means a Listen learns of an
    # advanced `since` within milliseconds of the compactor
    # committing, and the Listen-path halt (read.rs:298) wins every
    # race against the fetch-path "lost lease?" panic. Turn that
    # push off so subscribers have to discover the new state by
    # polling consensus on the retry timer below.
    "persist_pubsub_push_diff_enabled": "false",
    # Make the listen retry timer slower so the polling-based
    # discovery happens much less often. Combined with the (slow)
    # blob baseline + slowdown daemon, this gives the BatchFetcher
    # plenty of time to fetch an actually-GC'd part - and panic with
    # the specific "lost lease?" message - before Listen polls
    # consensus and notices the same expiry.
    "persist_next_listen_batch_retryer_clamp": "60s",
    "persist_next_listen_batch_retryer_fixed_sleep": "10s",
    "persist_next_listen_batch_retryer_initial_backoff": "5s",
}

SERVICES = [
    Cockroach(setup_materialize=True, in_memory=True),
    Postgres(),
    PostgresMetadata(),
    MySql(),
    SqlServer(),
    PolarisBootstrap(),
    Polaris(),
    Zookeeper(),
    Kafka(
        auto_create_topics=False,
        ports=["30123:30123"],
        allow_host_ports=True,
        environment_extra=[
            "KAFKA_ADVERTISED_LISTENERS=HOST://127.0.0.1:30123,PLAINTEXT://kafka:9092",
            "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=HOST:PLAINTEXT,PLAINTEXT:PLAINTEXT",
        ],
    ),
    SchemaRegistry(),
    Minio(setup_materialize=True, additional_directories=["copytos3"]),
    Azurite(),
    Mc(),
    Materialized(),
    Materialized(name="materialized2"),
    MzComposeService("sqlsmith", {"mzbuild": "sqlsmith"}),
    MzComposeService(
        name="persistcli",
        config={"mzbuild": "jobs"},
    ),
    Toxiproxy(),
    Testdrive(no_reset=True),
]


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    parse_common_args(parser)
    args = parser.parse_args()

    print(f"--- Random seed is {args.seed}")
    service_names = [
        "postgres",
        "mysql",
        "sql-server",
        "zookeeper",
        "kafka",
        "schema-registry",
        # Still required for backups/s3 testing even when we use Azurite as blob store
        "minio",
        "materialized",
    ]

    random.seed(args.seed)
    scenario = Scenario(args.scenario)
    complexity = Complexity(args.complexity)
    sanity_restart = False

    external = scenario in (
        Scenario.ZeroDowntimeDeploy,
        Scenario.BackupRestore,
        Scenario.Kill,
    )

    metadata_store = "cockroach" if external else "postgres-metadata"
    service_names.append(metadata_store)

    system_params = {
        **ADDITIONAL_SYSTEM_PARAMETER_DEFAULTS,
        **TOXIPROXY_SYSTEM_PARAMS,
    }

    with c.override(
        Materialized(
            external_blob_store=external,
            blob_store_is_azure=args.azurite,
            external_metadata_store="toxiproxy",
            metadata_store=metadata_store,
            ports=["6975:6875", "6976:6876", "6977:6877"],
            sanity_restart=sanity_restart,
            default_replication_factor=1,
            additional_system_parameter_defaults=system_params,
        ),
        Toxiproxy(seed=random.randrange(2**63)),
    ):
        # Bring up toxiproxy + the metadata store and install proxies
        # (with a small steady latency) in front of consensus and the
        # blob stores. setup_blob_toxiproxy fixes a long-standing
        # local bug in this file where the azurite proxy got no toxic.
        setup_consensus_toxiproxy(
            c,
            metadata_store=metadata_store,
            latency_ms=CONSENSUS_BASELINE_LATENCY_MS,
            jitter_ms=CONSENSUS_BASELINE_JITTER_MS,
        )
        c.up("minio", "azurite")
        setup_blob_toxiproxy(
            c,
            latency_ms=BLOB_BASELINE_LATENCY_MS,
            jitter_ms=BLOB_BASELINE_JITTER_MS,
        )

        c.up(
            *service_names,
            Service("polaris-bootstrap", idle=True),
            Service("polaris", idle=True),
        )
        setup_sql_server_testing(c)

        c.up(Service("mc", idle=True))
        c.exec(
            "mc",
            "mc",
            "alias",
            "set",
            "persist",
            "http://minio:9000/",
            "minioadmin",
            "minioadmin",
        )
        c.exec("mc", "mc", "version", "enable", "persist/persist")

        # Materialized is up; start two chaos daemons.
        # Each is seeded from args.seed for reproducibility.
        #
        # The consensus daemon does FULL BLACKOUTS: it disables the
        # consensus proxy for 12-30s, which is longer than the 10s
        # reader lease, so any reader's lease expires while the proxy
        # is down. This is the trigger for the lost-lease race.
        #
        # The blob daemon does SLOWDOWNS, not blackouts. Blob has to
        # stay reachable for the BatchFetcher "lost lease?" panic to
        # actually fire: the compactor needs to delete GC'd parts
        # through blob, and the reader needs to receive a real 404
        # (not a connection error) on retry. A fully-disabled blob
        # would prevent both of those. So instead we crank the
        # latency up to 6s for a stretch, which keeps readers in
        # active fetch state long enough to overlap a consensus
        # blackout without blocking GC.
        blob_proxy = (
            AZURITE_PROXY_NAME if args.azurite else MINIO_PROXY_NAME
        )
        start_disruption_daemon(
            c,
            rng=random.Random(f"{args.seed}-chaos-consensus"),
            proxy=CONSENSUS_PROXY_NAME,
            interval_range=(30.0, 90.0),
            duration_range=(12.0, 30.0),
            disruption_probability=0.5,
        )
        start_slowdown_daemon(
            c,
            rng=random.Random(f"{args.seed}-chaos-blob"),
            proxy=blob_proxy,
            baseline_latency_ms=BLOB_BASELINE_LATENCY_MS,
            baseline_jitter_ms=BLOB_BASELINE_JITTER_MS,
            slow_latency_ms=BLOB_SLOWDOWN_LATENCY_MS,
            slow_jitter_ms=BLOB_SLOWDOWN_JITTER_MS,
            interval_range=(20.0, 60.0),
            duration_range=(20.0, 45.0),
            slowdown_probability=0.7,
        )

        ports = {s: c.default_port(s) for s in service_names}
        ports["http"] = c.port("materialized", 6876)
        ports["mz_system"] = c.port("materialized", 6877)
        if scenario == Scenario.ZeroDowntimeDeploy:
            ports["materialized2"] = 7075
            ports["http2"] = 7076
            ports["mz_system2"] = 7077
        run(
            "127.0.0.1",
            ports,
            args.seed,
            args.runtime,
            complexity,
            scenario,
            args.threads,
            args.naughty_identifiers,
            args.replicas,
            c,
            args.azurite,
            sanity_restart,
        )
        # Don't wait for potentially hanging threads that we are ignoring
        os._exit(0)
