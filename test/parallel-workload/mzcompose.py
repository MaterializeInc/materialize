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

import requests

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
    Toxiproxy,
    set_consensus_latency,
    setup_consensus_toxiproxy,
    start_consensus_chaos_daemon,
    start_latency_chaos_daemon,
)
from materialize.mzcompose.services.zookeeper import Zookeeper
from materialize.parallel_workload.parallel_workload import parse_common_args, run
from materialize.parallel_workload.settings import (
    ADDITIONAL_SYSTEM_PARAMETER_DEFAULTS,
    Complexity,
    Scenario,
)

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

    # Pick a randomized "runtime" consensus latency for this seed.
    # Bootstrap *and* the parallel_workload setup phase (system ALTER +
    # Database creation) run at 0 latency for speed; we ramp these in
    # via the on_setup_complete callback below, right before the worker
    # threads start.
    #
    # The ranges are deliberately aggressive: we want to reproduce
    # persist races like "lost lease?" panics, which require the window
    # between a clusterd's lease registration and the environmentd
    # compactor's next consensus read to be wide enough that the
    # compactor can act on a stale view. Combined with the chaos
    # daemons below (which inject multi-second spikes on top of this
    # baseline), the upper end of the range approaches the persist
    # lease TTL.
    consensus_latency_ms = random.randint(500, 3000)
    consensus_jitter_ms = random.randint(200, 1000)

    # Same idea for blob storage. A snapshot that takes longer to
    # fully fetch than the lease lives is the most direct route to
    # the "could not fetch batch part" panic.
    blob_latency_ms = random.randint(500, 3000)
    blob_jitter_ms = random.randint(200, 1000)

    with c.override(
        Materialized(
            external_blob_store=external,
            blob_store_is_azure=args.azurite,
            external_metadata_store="toxiproxy",
            metadata_store=metadata_store,
            ports=["6975:6875", "6976:6876", "6977:6877"],
            sanity_restart=sanity_restart,
            default_replication_factor=1,
            additional_system_parameter_defaults=ADDITIONAL_SYSTEM_PARAMETER_DEFAULTS,
        ),
        Toxiproxy(seed=random.randrange(2**63)),
    ):
        toxiproxy_start(c, metadata_store)
        c.up(
            *service_names,
            Service("polaris-bootstrap", idle=True),
            Service("polaris", idle=True),
        )
        setup_sql_server_testing(c)

        def ramp_consensus_latency() -> None:
            print(
                f"--- parallel_workload setup complete; bumping consensus "
                f"latency to {consensus_latency_ms}ms (jitter "
                f"{consensus_jitter_ms}ms), blob latency to "
                f"{blob_latency_ms}ms (jitter {blob_jitter_ms}ms)"
            )
            set_consensus_latency(c, consensus_latency_ms, consensus_jitter_ms)
            _update_blob_latency(c, blob_latency_ms, blob_jitter_ms)
            # Start periodic latency spikes against both consensus and
            # blob. The spikes are what actually exposes "lost lease?"
            # style races: the writer needs a sudden propagation delay
            # to act on a stale view of consensus, not a steady one.
            # Seed both daemons from args.seed so the chaos schedule is
            # reproducible per run.
            chaos_rng = random.Random(f"{args.seed}-chaos")
            start_consensus_chaos_daemon(
                c,
                rng=chaos_rng,
                baseline_latency_ms=consensus_latency_ms,
                baseline_jitter_ms=consensus_jitter_ms,
            )
            start_latency_chaos_daemon(
                name="blob",
                set_latency=lambda lat, jit: _update_blob_latency(c, lat, jit),
                rng=random.Random(f"{args.seed}-chaos-blob"),
                baseline_latency_ms=blob_latency_ms,
                baseline_jitter_ms=blob_jitter_ms,
            )

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

        ports = {s: c.default_port(s) for s in service_names}
        ports["http"] = c.port("materialized", 6876)
        ports["mz_system"] = c.port("materialized", 6877)
        if scenario == Scenario.ZeroDowntimeDeploy:
            ports["materialized2"] = 7075
            ports["http2"] = 7076
            ports["mz_system2"] = 7077
        # try:
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
            on_setup_complete=ramp_consensus_latency,
        )
        # Don't wait for potentially hanging threads that we are ignoring
        os._exit(0)
        # TODO: Only ignore errors that will be handled by parallel-workload, not others
        # except Exception:
        #     print("--- Execution of parallel-workload failed")
        #     print_exc()
        #     # Don't fail the entire run. We ran into a crash,
        #     # ci-annotate-errors will handle this if it's an unknown failure.
        #     return


def _update_blob_latency(c: Composition, latency_ms: int, jitter_ms: int) -> None:
    """Update the latency toxics on the minio + azurite proxies in place."""
    port = c.default_port("toxiproxy")
    for proxy in ("minio", "azurite"):
        # The toxic name matches the proxy name (see toxiproxy_start).
        r = requests.post(
            f"http://localhost:{port}/proxies/{proxy}/toxics/{proxy}",
            json={"attributes": {"latency": latency_ms, "jitter": jitter_ms}},
        )
        assert r.status_code == 200, r.text


def toxiproxy_start(c: Composition, metadata_store: str) -> None:
    # Bring up toxiproxy + the metadata store and create a pass-through
    # proxy in front of consensus. We start with 0 latency so materialized
    # bootstraps quickly; workflow_default ramps this in via
    # set_consensus_latency once materialized is healthy.
    setup_consensus_toxiproxy(c, metadata_store=metadata_store)

    port = c.default_port("toxiproxy")
    r = requests.post(
        f"http://localhost:{port}/proxies",
        json={
            "name": "minio",
            "listen": "0.0.0.0:9000",
            "upstream": "minio:9000",
            "enabled": True,
        },
    )
    assert r.status_code == 201, r
    r = requests.post(
        f"http://localhost:{port}/proxies",
        json={
            "name": "azurite",
            "listen": "0.0.0.0:10000",
            "upstream": "azurite:10000",
            "enabled": True,
        },
    )
    assert r.status_code == 201, r
    # Install a latency toxic on each blob proxy. Start at 0 so blob is
    # effectively pass-through during bootstrap; workflow_default ramps
    # these in via _update_blob_latency once materialized is healthy.
    # Toxic name == proxy name (see _update_blob_latency).
    #
    # Note: prior to this point the azurite proxy used to get no toxic
    # at all (a copy-paste bug landed the second toxic on /proxies/minio
    # instead of /proxies/azurite). Fixed.
    for proxy in ("minio", "azurite"):
        r = requests.post(
            f"http://localhost:{port}/proxies/{proxy}/toxics",
            json={
                "name": proxy,
                "type": "latency",
                "attributes": {"latency": 0, "jitter": 0},
            },
        )
        assert r.status_code == 200, r.text
