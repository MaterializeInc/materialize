# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Test various Confluent Platform d Redpanda versions to make sure they are all
working with Materialize.
"""

from materialize import buildkite
from materialize.mzcompose import DEFAULT_CONFLUENT_PLATFORM_VERSION
from materialize.mzcompose.composition import Composition
from materialize.mzcompose.services.kafka import Kafka
from materialize.mzcompose.services.localstack import Localstack
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.redpanda import REDPANDA_VERSION, Redpanda
from materialize.mzcompose.services.schema_registry import SchemaRegistry
from materialize.mzcompose.services.testdrive import Testdrive
from materialize.mzcompose.services.zookeeper import Zookeeper

REDPANDA_VERSIONS = [
    "v22.3.25",
    "v23.1.21",
    "v23.2.29",
    "v23.3.21",
    "v24.1.18",
    "v24.2.12",
    REDPANDA_VERSION,
    "latest",
]

CONFLUENT_PLATFORM_VERSIONS = [
    "7.0.16",
    "7.1.14",
    "7.2.12",
    "7.3.10",
    "7.4.7",
    "7.5.6",
    "7.6.3",
    DEFAULT_CONFLUENT_PLATFORM_VERSION,
    "latest",
]

SERVICES = [
    Materialized(),
    # Occasional timeouts in CI with 60s timeout
    Testdrive(
        volumes_extra=["../testdrive:/workdir/testdrive"], default_timeout="120s"
    ),
    Redpanda(),
    Zookeeper(),
    Kafka(),
    SchemaRegistry(),
    Localstack(),
]


TD_CMD = [
    f"--var=default-replica-size={Materialized.Size.DEFAULT_SIZE}-{Materialized.Size.DEFAULT_SIZE}",
    f"--var=default-storage-size={Materialized.Size.DEFAULT_SIZE}-1",
    "--var=single-replica-cluster=quickstart",
    *[f"testdrive/{td}" for td in ["kafka-sinks.td", "kafka-upsert-sources.td"]],
]


def workflow_default(c: Composition) -> None:
    c.up("localstack")

    redpanda_versions = buildkite.shard_list(REDPANDA_VERSIONS, lambda v: v)
    print(
        f"Redpanda versions in shard with index {buildkite.get_parallelism_index()}: {redpanda_versions}"
    )

    for redpanda_version in redpanda_versions:
        print(f"--- Testing Redpanda {redpanda_version}")
        with c.override(Redpanda(version=redpanda_version)):
            c.down(destroy_volumes=True)
            c.up("redpanda", "materialized")
            c.run_testdrive_files(*TD_CMD)

    confluent_versions = buildkite.shard_list(CONFLUENT_PLATFORM_VERSIONS, lambda v: v)
    print(
        f"Confluent Platform versions in shard with index {buildkite.get_parallelism_index()}: {confluent_versions}"
    )

    for confluent_version in confluent_versions:
        print(f"--- Testing Confluent Platform {confluent_version}")
        with c.override(
            Zookeeper(tag=confluent_version),
            Kafka(tag=confluent_version),
            SchemaRegistry(tag=confluent_version),
        ):
            c.down(destroy_volumes=True)
            c.up("zookeeper", "kafka", "schema-registry", "materialized")
            c.run_testdrive_files(*TD_CMD)
