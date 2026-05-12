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
  - materialized      : the SUT (environmentd; clusterd is external)
  - workload          : Python test driver wired to the Antithesis SDK

Usage:
  bin/mzcompose --find antithesis run default                       # bring up the cluster
  bin/pyactivate test/antithesis/export-compose.py > config/...     # dump compose YAML
"""

from materialize.mzcompose.composition import Composition
from materialize.mzcompose.service import Service, ServiceConfig
from materialize.mzcompose.services.clusterd import Clusterd
from materialize.mzcompose.services.kafka import Kafka
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.minio import Minio
from materialize.mzcompose.services.mysql import MySql, create_mysql_server_args
from materialize.mzcompose.services.postgres import PostgresMetadata
from materialize.mzcompose.services.schema_registry import SchemaRegistry
from materialize.mzcompose.services.zookeeper import Zookeeper


class Workload(Service):
    """Antithesis workload client — Python test driver."""

    def __init__(self) -> None:
        config: ServiceConfig = {
            "mzbuild": "antithesis-workload",
            "depends_on": {
                "materialized": {"condition": "service_healthy"},
                "clusterd1": {"condition": "service_started"},
                "clusterd2": {"condition": "service_started"},
                "kafka": {"condition": "service_healthy"},
                "schema-registry": {"condition": "service_started"},
                "mysql": {"condition": "service_healthy"},
                "mysql-replica": {"condition": "service_healthy"},
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
                # MySQL primary and replica connection details.
                "MYSQL_HOST=mysql",
                "MYSQL_REPLICA_HOST=mysql-replica",
                f"MYSQL_PASSWORD={MySql.DEFAULT_ROOT_PASSWORD}",
            ],
        }
        super().__init__(name="workload", config=config)


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
    # Two clusterd processes, one per replica of the unmanaged
    # `antithesis_cluster`. Provisioning both replicas in the same cluster
    # exercises multi-replica source ingestion and compute paths
    # (notably the `compute-replica-epoch-isolation` property), and lets
    # Antithesis kill either replica's backing container without taking
    # the workload offline.
    #
    # `workers=4` per clusterd means each replica runs four timely worker
    # threads in one process. The extra intra-process parallelism is the
    # surface area Antithesis's thread-pausing fault targets — with a
    # single worker, "pause one thread" effectively pauses the whole
    # process, which the container-pause fault already covers. The matching
    # `WORKERS 4` in the CREATE CLUSTER REPLICAS statement must stay in
    # lockstep with this value (it's read by the controller, not by
    # clusterd).
    #
    # Each clusterd MUST have its own /scratch volume — the upsert
    # operator's RocksDB state lives there and takes an exclusive file
    # lock per worker (`/scratch/storage/upsert/<id>/<worker>/LOCK`).
    # The DEFAULT_MZ_VOLUMES list uses a single named volume
    # `scratch:/scratch` shared across containers; passing per-instance
    # named volumes (`clusterd1_scratch`, `clusterd2_scratch`) keeps the
    # locks separate while leaving the other volumes shared. Found via
    # an Antithesis run where clusterd1 deadlocked retrying to open
    # `/scratch/storage/upsert/u3/0/LOCK` because clusterd2 held it,
    # which then drove a continuous suspend-and-restart loop that
    # corrupted the upsert state.
    Clusterd(
        name="clusterd1",
        workers=4,
        volumes=[
            "mzdata:/mzdata",
            "mydata:/var/lib/mysql-files",
            "tmp:/share/tmp",
            "clusterd1_scratch:/scratch",
        ],
    ),
    Clusterd(
        name="clusterd2",
        workers=4,
        volumes=[
            "mzdata:/mzdata",
            "mydata:/var/lib/mysql-files",
            "tmp:/share/tmp",
            "clusterd2_scratch:/scratch",
        ],
    ),
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
    Workload(),
]


def workflow_default(c: Composition) -> None:
    """Bring up the Antithesis test cluster."""
    c.up(
        "postgres-metadata",
        "minio",
        "zookeeper",
        "kafka",
        "schema-registry",
        "clusterd1",
        "clusterd2",
        "mysql",
        "mysql-replica",
    )
    c.up("materialized")
    c.up("workload")
