# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize.mzcompose import Workflow
from materialize.mzcompose.services import (
    Kafka,
    Materialized,
    SchemaRegistry,
    Testdrive,
    Zookeeper,
)

CONFLUENT_PLATFORM_VERSIONS = [
    "4.0.0",
    "5.0.0",
    "6.0.2",
    "6.1.2",
    "6.2.0",
    "latest",
]

services = [
    Materialized(),
    Testdrive(),
    Zookeeper(),
    Kafka(),
    SchemaRegistry(),
]


def workflow_kafka_matrix(w: Workflow) -> None:
    for version in CONFLUENT_PLATFORM_VERSIONS:
        print(f"==> Testing Confluent Platform {version}")
        confluent_platform_services = [
            Zookeeper(tag=version),
            Kafka(tag=version),
            SchemaRegistry(tag=version),
        ]
        with w.with_services(confluent_platform_services):
            w.start_and_wait_for_tcp(
                services=["zookeeper", "kafka", "schema-registry", "materialized"]
            )
            w.wait_for_mz()
            w.run_service(
                service="testdrive-svc",
                command="kafka-matrix.td",
            )
            w.remove_services(
                services=["zookeeper", "kafka", "schema-registry", "materialized"],
                destroy_volumes=True,
            )
