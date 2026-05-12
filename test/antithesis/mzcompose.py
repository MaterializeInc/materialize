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
            ],
        }
        super().__init__(name="workload", config=config)


SERVICES = [
    PostgresMetadata(),
    Minio(setup_materialize=True),
    Zookeeper(),
    Kafka(auto_create_topics=True),
    SchemaRegistry(),
    # Two clusterd processes, one per replica of the unmanaged
    # `antithesis_cluster`. Provisioning both replicas in the same cluster
    # exercises multi-replica source ingestion and compute paths
    # (notably the `compute-replica-epoch-isolation` property), and lets
    # Antithesis kill either replica's backing container without taking
    # the workload offline.
    Clusterd(name="clusterd1"),
    Clusterd(name="clusterd2"),
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
    )
    c.up("materialized")
    c.up("workload")
