# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize.mzcompose.composition import Composition
from materialize.mzcompose.services.kafka import Kafka
from materialize.mzcompose.services.localstack import Localstack
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.redpanda import Redpanda
from materialize.mzcompose.services.schema_registry import SchemaRegistry
from materialize.mzcompose.services.testdrive import Testdrive
from materialize.mzcompose.services.zookeeper import Zookeeper

REDPANDA_VERSIONS = ["v22.3.25", "v23.1.21", "v23.2.25", "v23.3.5"]

CONFLUENT_PLATFORM_VERSIONS = [
    "6.2.14",
    "7.0.13",
    "7.1.11",
    "7.2.9",
    "7.3.7",
    "7.4.4",
    "7.5.2",
    "latest",
]

SERVICES = [
    Materialized(),
    Testdrive(volumes_extra=["../testdrive:/workdir/testdrive"], default_timeout="60s"),
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

    for redpanda_version in REDPANDA_VERSIONS:
        print(f"--- Testing Redpanda {redpanda_version}")
        with c.override(Redpanda(version=redpanda_version)):
            c.down(destroy_volumes=True)
            c.up("redpanda", "materialized")
            c.run_testdrive_files(*TD_CMD)

    for confluent_version in CONFLUENT_PLATFORM_VERSIONS:
        print(f"--- Testing Confluent Platform {confluent_version}")
        # No arm64 images available for Confluent Platform versions 6.*
        platform = "linux/amd64" if confluent_version.startswith("6.") else None
        with c.override(
            Zookeeper(tag=confluent_version, platform=platform),
            Kafka(tag=confluent_version, platform=platform),
            SchemaRegistry(tag=confluent_version, platform=platform),
        ):
            c.down(destroy_volumes=True)
            c.up("zookeeper", "kafka", "schema-registry", "materialized")
            c.run_testdrive_files(*TD_CMD)
