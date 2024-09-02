# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Functional test against Kafka with 3 brokers, most of our other tests only use
a single Broker.
"""

import time

from materialize.mzcompose.composition import Composition
from materialize.mzcompose.services.kafka import Kafka
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.schema_registry import SchemaRegistry
from materialize.mzcompose.services.testdrive import Testdrive
from materialize.mzcompose.services.zookeeper import Zookeeper

SERVICES = [
    Zookeeper(),
    Kafka(name="kafka1", broker_id=1, offsets_topic_replication_factor=2),
    Kafka(name="kafka2", broker_id=2, offsets_topic_replication_factor=2),
    Kafka(name="kafka3", broker_id=3, offsets_topic_replication_factor=2),
    SchemaRegistry(
        kafka_servers=[("kafka1", "9092"), ("kafka2", "9092"), ("kafka3", "9092")]
    ),
    Materialized(),
    Testdrive(
        entrypoint_extra=[
            "--kafka-option=acks=all",
        ],
        seed=1,
    ),
]


def workflow_default(c: Composition) -> None:
    c.up("zookeeper", "kafka1", "kafka2", "kafka3", "schema-registry", "materialized")
    c.run_testdrive_files("--kafka-addr=kafka2", "01-init.td")
    time.sleep(10)
    c.kill("kafka1")
    time.sleep(10)
    c.run_testdrive_files(
        "--kafka-addr=kafka2,kafka3", "--no-reset", "02-after-leave.td"
    )
    c.up("kafka1")
    time.sleep(10)
    c.run_testdrive_files("--kafka-addr=kafka1", "--no-reset", "03-after-join.td")
