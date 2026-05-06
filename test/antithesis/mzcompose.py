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

Defines the minimal topology needed to exercise Materialize under Antithesis:
  - postgres-metadata: consensus/catalog store
  - minio: S3-compatible blob storage for persist
  - redpanda: Kafka-compatible broker for source ingestion
  - materialized: the SUT (embedded clusterd mode)
  - workload: Python test driver with Antithesis SDK

Usage:
  bin/mzcompose --find antithesis run default        # bring up the cluster
  bin/mzcompose --find antithesis run export-compose  # dump compose YAML
"""

import sys

import yaml

from materialize.mzcompose.composition import Composition, WorkflowArgumentParser
from materialize.mzcompose.service import Service, ServiceConfig
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.minio import Minio
from materialize.mzcompose.services.postgres import PostgresMetadata
from materialize.mzcompose.services.redpanda import Redpanda


class Workload(Service):
    """Antithesis workload client — Python test driver."""

    def __init__(self) -> None:
        config: ServiceConfig = {
            "mzbuild": "antithesis-workload",
            "depends_on": {
                "materialized": {"condition": "service_healthy"},
                "redpanda": {"condition": "service_healthy"},
            },
            "environment": [
                "PGHOST=materialized",
                "PGPORT=6875",
                "PGUSER=materialize",
                "KAFKA_BROKER=kafka:9092",
                "SCHEMA_REGISTRY_URL=http://schema-registry:8081",
            ],
        }
        super().__init__(name="workload", config=config)


SERVICES = [
    PostgresMetadata(),
    Minio(setup_materialize=True),
    Redpanda(auto_create_topics=True),
    Materialized(
        external_blob_store=True,
        external_metadata_store=True,
        metadata_store="postgres-metadata",
        unsafe_mode=True,
        soft_assertions=True,
        sanity_restart=False,
    ),
    Workload(),
]


def workflow_default(c: Composition) -> None:
    """Bring up the Antithesis test cluster."""
    c.up("postgres-metadata", "minio", "redpanda")
    c.up("materialized")
    c.up("workload")


def workflow_export_compose(c: Composition) -> None:
    """Export the resolved docker-compose YAML to stdout.

    Usage:
      bin/mzcompose --find antithesis run export-compose > antithesis/config/docker-compose.yaml
    """
    # c.compose is the fully-resolved compose dict (mzbuild: replaced with image:)
    yaml.dump(c.compose, sys.stdout, default_flow_style=False, sort_keys=False)
