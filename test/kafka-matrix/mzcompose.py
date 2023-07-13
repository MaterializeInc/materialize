# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize.mzcompose import Composition
from materialize.mzcompose.services import (
    Kafka,
    Localstack,
    Materialized,
    Redpanda,
    SchemaRegistry,
    Testdrive,
    Zookeeper,
)

REDPANDA_VERSIONS = ["v23.1.2", "v22.2.11", "v22.1.11"]

CONFLUENT_PLATFORM_VERSIONS = [
    "6.2.8",
    "7.0.7",
    "7.1.5",
    "7.2.3",
    "7.3.2",
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
    f"--var=default-storage-size={Materialized.Size.DEFAULT_SIZE}-1",
    *[f"testdrive/{td}" for td in ["kafka-sinks.td", "kafka-upsert-sources.td"]],
]


def workflow_default(c: Composition) -> None:
    c.up("localstack")

    for redpanda_version in REDPANDA_VERSIONS:
        print(f"--- Testing Redpanda {redpanda_version}")
        with c.override(Redpanda(version=redpanda_version)):
            c.down(destroy_volumes=True)
            c.up("redpanda", "materialized")
            c.run("testdrive", *TD_CMD)

    for confluent_version in CONFLUENT_PLATFORM_VERSIONS:
        print(f"--- Testing Confluent Platform {confluent_version}")
        with c.override(
            Zookeeper(tag=confluent_version),
            Kafka(tag=confluent_version),
            SchemaRegistry(tag=confluent_version),
        ):
            c.down(destroy_volumes=True)
            c.up("zookeeper", "kafka", "schema-registry", "materialized")
            c.run("testdrive", *TD_CMD)
