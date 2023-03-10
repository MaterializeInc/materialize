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
    Materialized,
    SchemaRegistry,
    TestCerts,
    Testdrive,
    Zookeeper,
)

SERVICES = [
    TestCerts(),
    Zookeeper(),
    Kafka(
        environment=[
            "KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181",
            # Setting the following values to 3 to trigger a failure
            # sets the transaction.state.log.min.isr config
            "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=3",
            # sets the transaction.state.log.replication.factor config
            "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=3"
        ],
    ),
    SchemaRegistry(),
    Materialized(),
    Testdrive(),
]


def workflow_default(c: Composition) -> None:
    c.up("test-certs")
    c.up("zookeeper", "kafka", "schema-registry")
    c.up("materialized")
    c.run("testdrive", "*.td")
