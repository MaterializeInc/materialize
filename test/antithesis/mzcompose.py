# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Antithesis test composition for Materialize.

Topology exercised under Antithesis:
  - postgres-metadata : consensus/catalog/timestamp-oracle store
  - minio             : S3-compatible blob storage for persist
  - zookeeper + kafka : Kafka broker for source ingestion
  - schema-registry   : Avro/Protobuf schemas for kafka sources
  - clusterd1, clusterd2 : two external compute+storage processes — each
                        backs one replica of `antithesis_cluster`, so
                        Antithesis killing either container exercises the
                        compute/storage-replica recovery and rebalancing
                        paths without taking the cluster offline.
  - clusterd-pool-{0..N-1} : a configurable pool of external clusterd
                        containers that the parallel-workload driver
                        claims one-per-cluster to give each
                        parallel-workload cluster its own container.
                        Without this pool, parallel-workload clusters
                        would all share materialized's process orchestrator
                        and Antithesis could only fault the entire
                        container as a unit. Pool size is controlled by
                        the `ANTITHESIS_CLUSTERD_POOL_SIZE` env var (read
                        from the harness; defaults to 8).
  - materialized      : the SUT (environmentd; clusterd is external)
  - workload          : Python test driver wired to the Antithesis SDK
  - fault-orchestrator : single bash container alternating quiet and
                        faulting windows globally via
                        `ANTITHESIS_STOP_FAULTS`. Centralising the
                        cadence avoids the failure mode where every
                        driver requests its own quiet window and the
                        union of overlapping requests keeps the system
                        in a quiet state most of the time.

Usage:
  bin/mzcompose --find antithesis run default                       # bring up the cluster
  bin/pyactivate test/antithesis/export-compose.py > config/...     # dump compose YAML
"""

import json
import os
from pathlib import Path

from materialize.mzcompose import cluster_replica_size_map
from materialize.mzcompose.composition import Composition
from materialize.mzcompose.service import Service, ServiceConfig
from materialize.mzcompose.services.clusterd import Clusterd
from materialize.mzcompose.services.kafka import Kafka
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.minio import Minio
from materialize.mzcompose.services.mysql import MySql, create_mysql_server_args
from materialize.mzcompose.services.polaris import Polaris, PolarisBootstrap
from materialize.mzcompose.services.postgres import Postgres, PostgresMetadata
from materialize.mzcompose.services.schema_registry import SchemaRegistry
from materialize.mzcompose.services.zookeeper import Zookeeper

# Number of pool clusterd containers reserved for parallel-workload clusters
# (one container per cluster, giving each its own container-level fault
# domain). Read from the env so CI/local runs can tune it without editing
# this file. Default 2 — the no-lock allocator (rng-picked slot per
# invocation) tolerates oversubscription, and a smaller pool keeps the
# topology closer to production replica counts.
CLUSTERD_POOL_SIZE = int(os.environ.get("ANTITHESIS_CLUSTERD_POOL_SIZE", "2"))

# Timely worker threads per clusterd process. Reverted from 16 back to 4
# on suspicion that Antithesis's deterministic hypervisor runs the whole
# fleet on a single core — 16 work-stealing Timely workers per process
# on one core would burn most of their wakeups on context-switch
# overhead and starve dependent steps, which would manifest as
# workloads never finishing.
#
# This value must stay in lockstep with the `WORKERS N` clause in every
# CREATE CLUSTER REPLICAS statement that targets these containers
# (workload-entrypoint.sh reads it from the CLUSTERD_WORKERS env var
# the Workload service passes through; the parallel-workload Python
# driver consumes the same env via the framework's pool-cluster
# wrapper).
CLUSTERD_WORKERS = 4


class FaultOrchestrator(Service):
    """Single bash container that drives Antithesis fault windows globally.

    Invokes `${ANTITHESIS_STOP_FAULTS} <seconds>` to open quiet windows,
    then sleeps through faults-ON windows, on a randomised cadence
    (MIN_ON..MAX_ON / MIN_OFF..MAX_OFF). The script is bundled in
    `test/antithesis/fault-orchestrator/pause_faults.sh` and inlined into
    the compose `command:` here so we don't need a new mzbuild image
    just to ship 30 lines of bash.

    The Antithesis engagement team flagged per-driver quiet-period
    requests as an anti-pattern: with many concurrent drivers each
    asking for a quiet window, the union of overlapping windows leaves
    the SUT mostly un-faulted. Centralising the cadence here means
    faults arrive in one coordinated rhythm; drivers stay robust to
    quiet/faulting transitions by relying on `wait_for_catchup` with
    generous timeouts.

    Outside Antithesis `ANTITHESIS_STOP_FAULTS` is unset and the script
    exits immediately, so this service is a no-op for local validate.
    """

    def __init__(self) -> None:
        script_path = Path(__file__).parent / "fault-orchestrator" / "pause_faults.sh"
        # Compose interpolates `${VAR}` in every string value at parse
        # time, which would eat the script's shell variable references
        # (`${RANDOM}`, `${MIN_ON}`, `${ANTITHESIS_STOP_FAULTS}`, etc.)
        # before bash ever sees them. Double the `$` to pass through a
        # literal `$` and let bash do its own expansion at runtime. The
        # underlying .sh file stays normal so shellcheck and direct
        # execution work.
        script = script_path.read_text().replace("$", "$$")
        config: ServiceConfig = {
            # bash:5 is alpine-based and ships `bash`, `od`, `tr`, and
            # `sleep` via busybox — everything the script uses. Public
            # image, so it sails through export-compose.py untouched.
            "image": "bash:5",
            # `bash -c <script>` runs the script string. Earlier this used
            # `bash -s`, which reads commands from stdin — in a docker
            # container there's nothing on stdin so bash exited cleanly
            # with no output and the orchestrator silently no-op'd. Net
            # effect: Antithesis fault injection ran unconstrained with
            # no quiet windows ever opening, which starved every driver
            # that needed more than one connection.
            "entrypoint": ["bash", "-c"],
            "command": [script],
            "environment": [
                # Defaults chosen so MAX_ON stays well under the smallest
                # driver's CATCHUP_TIMEOUT_S (currently 90s) — every
                # driver lifetime has a chance to span at least one quiet
                # window.
                "START_DELAY=30",
                "MIN_ON=20",
                "MAX_ON=40",
                "MIN_OFF=20",
                "MAX_OFF=40",
            ],
            # Wait for materialized so the orchestrator's first
            # ANTITHESIS_STOP_FAULTS call doesn't precede the SUT being
            # ready. Timing is not safety-critical: Antithesis only
            # starts injecting faults after setup-complete fires from
            # the workload container.
            "depends_on": {
                "materialized": {"condition": "service_healthy"},
            },
            "restart": "no",
        }
        super().__init__(name="fault-orchestrator", config=config)


class Workload(Service):
    """Antithesis workload client — Python test driver."""

    def __init__(self) -> None:
        config: ServiceConfig = {
            # `antithesis-workload-combined` is the kitchen-sink build of
            # the per-group workload images.  Interactive `bin/mzcompose
            # --find antithesis` flows expect a working mzbuild ref here.
            # When `export-compose.py` emits a per-group docker-compose
            # YAML it rewrites this to `antithesis-workload-<group>` for
            # that group's image; mzcompose runs default to combined.
            "mzbuild": "antithesis-workload-combined",
            "depends_on": {
                "materialized": {"condition": "service_healthy"},
                "clusterd1": {"condition": "service_started"},
                "clusterd2": {"condition": "service_started"},
                "kafka": {"condition": "service_healthy"},
                "schema-registry": {"condition": "service_started"},
                "mysql": {"condition": "service_healthy"},
                "mysql-replica": {"condition": "service_healthy"},
                "postgres-source": {"condition": "service_healthy"},
            },
            "environment": [
                "PGHOST=materialized",
                "PGPORT=6875",
                "PGUSER=materialize",
                # Internal SQL port for system-privileged setup (CREATE CLUSTER).
                "PGPORT_INTERNAL=6877",
                "PGUSER_INTERNAL=mz_system",
                "KAFKA_BROKER=kafka:9092",
                "SCHEMA_REGISTRY_URL=http://schema-registry:8081",
                # Name of the unmanaged cluster the workload-entrypoint
                # provisions against clusterd1 before emitting setup-complete.
                "MZ_ANTITHESIS_CLUSTER=antithesis_cluster",
                # Pool size for the long-lived `pool_cluster_{i}` clusters
                # the entrypoint bootstraps. Mirrored to the parallel-
                # workload driver (CLUSTERD_POOL_SIZE) so they agree on the
                # slot count.
                f"ANTITHESIS_CLUSTERD_POOL_SIZE={CLUSTERD_POOL_SIZE}",
                f"CLUSTERD_POOL_SIZE={CLUSTERD_POOL_SIZE}",
                # Worker count for the WORKERS clause in every CREATE
                # CLUSTER REPLICAS that targets a clusterd-pool or
                # clusterd1/2 container. Must match the `workers=`
                # argument passed to each `Clusterd(...)` Service above,
                # because the controller reads it from this clause not
                # from clusterd's runtime config.
                f"CLUSTERD_WORKERS={CLUSTERD_WORKERS}",
                # MySQL primary and replica connection details.
                "MYSQL_HOST=mysql",
                "MYSQL_REPLICA_HOST=mysql-replica",
                f"MYSQL_PASSWORD={MySql.DEFAULT_ROOT_PASSWORD}",
                # Postgres CDC upstream connection details. Materialize
                # talks to this PG directly via a logical replication slot
                # — production Postgres CDC is single-instance, unlike the
                # MySQL primary+replica topology above.
                "PG_SOURCE_HOST=postgres-source",
                "PG_SOURCE_PORT=5432",
                "PG_SOURCE_USER=postgres",
                "PG_SOURCE_PASSWORD=postgres",
                "PG_SOURCE_DATABASE=postgres",
                # The testdrive binary inside the workload image reads
                # this from the env (clap `env=CLUSTER_REPLICA_SIZES`)
                # and uses it for any `CREATE CLUSTER REPLICAS (... SIZE
                # '...')` statement in a checked-in `.td` file. Without
                # it, testdrive aborts at startup with "required argument
                # missing". The map matches what `materialized` is
                # actually configured with, so the size names a test
                # file references (`scale=1,workers=1`, `'1'`, ...) all
                # resolve.
                "CLUSTER_REPLICA_SIZES="
                + json.dumps(cluster_replica_size_map()),
            ],
        }
        super().__init__(name="workload", config=config)


def _polaris_bootstrap() -> PolarisBootstrap:
    """Polaris admin-tool bootstrap — runs once to provision the
    `POLARIS` realm in the backing JDBC store before the main polaris
    container starts.

    The upstream `PolarisBootstrap` ctor hardcodes a depends_on of the
    service named `postgres`, which doesn't exist as a real service in
    this composition.  We make `postgres-source` answer to the
    `postgres` hostname via an export-compose-time network alias (see
    `EXTRA_NETWORK_ALIASES` in export-compose.py), but `depends_on`
    resolves against service keys, not aliases.  Override it to point
    at `postgres-source` directly.
    """
    svc = PolarisBootstrap()
    svc.config["depends_on"] = {"postgres-source": {"condition": "service_started"}}
    return svc


def _polaris() -> Polaris:
    """Polaris (Iceberg REST catalog).  Listens on :8181 (api) / :8182
    (management).  Used by parallel_workload's CreateIcebergSinkAction —
    without polaris in the topology those actions silent-fail under
    Scenario.Kill, which means we never actually exercise the iceberg
    sink path.

    Like `PolarisBootstrap`, the upstream ctor's hardcoded `postgres`
    dependency is rewritten to point at `postgres-source` so docker-
    compose's depends_on graph resolves.

    The `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` extra env mirrors
    what upstream's `setup_polaris_for_iceberg` does when it
    `c.override(Polaris(extra_environment=[...]))`: Polaris uses these
    creds (in addition to whatever's in each catalog's `s3.*` properties)
    to read/write iceberg metadata in minio.  We use the static
    `minioadmin` root creds rather than provisioning a per-test minio
    user, since the Antithesis sandbox is single-tenant by run.
    """
    svc = Polaris(
        extra_environment=[
            "AWS_ACCESS_KEY_ID=minioadmin",
            "AWS_SECRET_ACCESS_KEY=minioadmin",
        ],
    )
    svc.config["depends_on"] = {
        "polaris-bootstrap": {"condition": "service_completed_successfully"},
        "postgres-source": {"condition": "service_started"},
    }
    return svc


SERVICES = [
    PostgresMetadata(),
    Minio(setup_materialize=True),
    Zookeeper(),
    Kafka(auto_create_topics=True),
    SchemaRegistry(),
    # MySQL primary — GTID-enabled. WRITESET binlog dependency tracking
    # is what lets the replica run parallel workers without losing commit
    # order; in MySQL 8.4+ WRITESET is the default and the explicit knob
    # was removed (`binlog_transaction_dependency_tracking` is unknown
    # past 8.4, and the antithesis image is `mysql:9.5.0`).
    MySql(
        use_seeded_image=False,
        volumes=[
            "mysqldata_primary:/var/lib/mysql",
            "mydata:/var/lib/mysql-files",
        ],
        additional_args=create_mysql_server_args(server_id="1", is_master=True),
    ),
    # MySQL replica — multithreaded replication (4 workers, commit-order
    # preserved).  Replication is configured at runtime by
    # first_mysql_replica_setup.py after both containers are healthy.
    MySql(
        name="mysql-replica",
        use_seeded_image=False,
        volumes=[
            "mysqldata_replica:/var/lib/mysql",
            "mydata:/var/lib/mysql-files",
        ],
        additional_args=create_mysql_server_args(server_id="2", is_master=False)
        + [
            "--replica_parallel_workers=4",
            "--replica_preserve_commit_order=ON",
        ],
    ),
    # Postgres source — single instance with logical replication enabled.
    # Materialize talks to this PG directly via a replication slot, which
    # is how PG CDC is deployed in production (unlike MySQL where Mz reads
    # from a replica). Separate from `postgres-metadata` so Antithesis
    # faults on the source path don't interfere with consensus storage.
    #
    # `wal_level=logical`, `max_wal_senders`, `max_replication_slots` are
    # set by Postgres' default ctor. `setup_materialize=False` keeps the
    # init scripts out — the PG-CDC setup driver creates the schema,
    # publication, and table at runtime.
    #
    # `max_slot_wal_keep_size=64MB` bounds how much WAL a stalled
    # replication slot is allowed to retain. With the default `-1`
    # (unlimited), `test/pg-cdc/max-slot-wal-keep-size.td` can't trigger
    # the slot-invalidation error it's checking for — the bulk insert
    # plus two full-table updates the test does just accumulate quietly,
    # Materialize catches up cleanly on resume, and the `! SELECT ...
    # contains:replication slot has been invalidated` assertion fails.
    # 64MB is small enough that the test's ~6GB of dirty pages overruns
    # it deterministically, large enough that no realistic non-test
    # workload trips it.
    Postgres(
        name="postgres-source",
        ports=["5432"],
        extra_command=["-c", "max_slot_wal_keep_size=64MB"],
    ),
    _polaris_bootstrap(),
    _polaris(),
    # Two clusterd processes, one per replica of the unmanaged
    # `antithesis_cluster`. Provisioning both replicas in the same cluster
    # exercises multi-replica source ingestion and compute paths
    # (notably the `compute-replica-epoch-isolation` property), and lets
    # Antithesis kill either replica's backing container without taking
    # the workload offline.
    #
    # `workers=CLUSTERD_WORKERS` (4) per clusterd means each replica runs
    # four timely worker threads in one process. Was bumped to 16 to
    # match production single-process density but reverted on suspicion
    # that Antithesis's single-core hypervisor turns 16-thread work-
    # stealing into a context-switch storm that starves progress. The
    # matching `WORKERS N` clause in every CREATE CLUSTER REPLICAS
    # statement must equal this — workload-entrypoint.sh reads
    # CLUSTERD_WORKERS from the env the Workload service exports.
    #
    # `scratch_directory=None` matches production: cluster replicas in
    # cloud deployments don't get a scratch disk, so the upsert operator's
    # RocksDB initializes with `Env::mem_env()` and stores its state
    # entirely in process memory. Passing a scratch directory would put
    # us on a code path production never exercises, and would also
    # require careful per-instance volume plumbing to avoid the two
    # clusterds racing on the same `/scratch/storage/upsert/<id>/<w>/LOCK`
    # file (which manifested as continuous Stalled/suspend-and-restart
    # loops on clusterd1 in an earlier run).
    Clusterd(
        name="clusterd1",
        workers=CLUSTERD_WORKERS,
        scratch_directory=None,
    ),
    Clusterd(
        name="clusterd2",
        workers=CLUSTERD_WORKERS,
        scratch_directory=None,
    ),
    # Pool of identical clusterd containers reserved for the
    # parallel-workload driver. Each instance backs one long-lived
    # `pool_cluster_<i>` (bootstrapped by workload-entrypoint.sh), giving
    # that cluster its own container-level fault domain (Antithesis can
    # kill / pause / partition / throttle a specific pool member without
    # affecting any other cluster). Same settings as clusterd1/clusterd2:
    # workers=CLUSTERD_WORKERS, no scratch (matches production),
    # restart=no so Antithesis fault injection isn't fought by docker-
    # compose.
    #
    # Pool sizing rationale lives in test/antithesis/workload/test/
    # parallel_driver_parallel_workload.py — the driver picks a slot at
    # random per invocation; with the no-lock allocator, multiple
    # invocations may share a pool cluster (which is fine because every
    # workload object lives in a seed-scoped database).
    *[
        Clusterd(
            name=f"clusterd-pool-{i}",
            workers=CLUSTERD_WORKERS,
            scratch_directory=None,
        )
        for i in range(CLUSTERD_POOL_SIZE)
    ],
    Materialized(
        external_blob_store=True,
        external_metadata_store=True,
        metadata_store="postgres-metadata",
        unsafe_mode=True,
        soft_assertions=True,
        sanity_restart=False,
        support_external_clusterd=True,
        # Allow creating an unmanaged cluster pointed at clusterd1 — without
        # this, CREATE CLUSTER ... STORAGECTL ADDRESSES is rejected.
        additional_system_parameter_defaults={
            "unsafe_enable_unorchestrated_cluster_replicas": "true",
        },
    ),
    FaultOrchestrator(),
    Workload(),
]


def workflow_default(c: Composition) -> None:
    """Bring up the Antithesis test cluster."""
    pool_services = [f"clusterd-pool-{i}" for i in range(CLUSTERD_POOL_SIZE)]
    c.up(
        "postgres-metadata",
        "minio",
        "zookeeper",
        "kafka",
        "schema-registry",
        "clusterd1",
        "clusterd2",
        *pool_services,
        "mysql",
        "mysql-replica",
        "postgres-source",
    )
    c.up("materialized")
    c.up("fault-orchestrator")
    c.up("workload")
