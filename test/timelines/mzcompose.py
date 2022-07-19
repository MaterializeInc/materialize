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

SERVICES = [
    Materialized(),
    Testdrive(volumes_extra=["../testdrive:/workdir/testdrive"]),
    Zookeeper(),
    Kafka(),
    SchemaRegistry(),
    Localstack(),
]


def workflow_default(c: Composition) -> None:
    for _ in range(3):
        c.start_and_wait_for_tcp(
            services=[
                "zookeeper",
                "kafka",
                "schema-registry",
                "materialized",
                "localstack",
            ]
        )
        c.wait_for_materialized()
        c.run("testdrive", "timelines.td")
        c.rm(
            "zookeeper",
            "kafka",
            "schema-registry",
            "materialized",
            destroy_volumes=True,
        )
