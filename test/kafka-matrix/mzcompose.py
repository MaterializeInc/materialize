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
    SchemaRegistry,
    Testdrive,
    Zookeeper,
)

CONFLUENT_PLATFORM_VERSIONS = [
    "5.5.11",
    "6.0.11",
    "6.1.9",
    "6.2.8",
    "7.0.7",
    "7.1.5",
    "7.2.3",
    "7.3.1",
    "latest",
]

SERVICES = [
    Materialized(),
    Testdrive(volumes_extra=["../testdrive:/workdir/testdrive"], default_timeout="60s"),
    Zookeeper(),
    Kafka(),
    SchemaRegistry(),
    Localstack(),
]


def workflow_default(c: Composition) -> None:
    c.up("localstack")

    for version in CONFLUENT_PLATFORM_VERSIONS:
        print(f"==> Testing Confluent Platform {version}")
        confluent_platform_services = [
            Zookeeper(tag=version),
            Kafka(tag=version),
            SchemaRegistry(tag=version),
        ]
        with c.override(*confluent_platform_services):
            c.up("zookeeper", "kafka", "schema-registry", "materialized")
            c.run("testdrive", "testdrive/kafka-*.td")
            c.kill(
                "zookeeper",
                "kafka",
                "schema-registry",
                "materialized",
            )
            c.rm(
                "zookeeper",
                "kafka",
                "schema-registry",
                "materialized",
                "testdrive",
                destroy_volumes=True,
            )
            c.rm_volumes("mzdata", force=True)
