# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize.mzcompose import Composition, WorkflowArgumentParser
from materialize.mzcompose.services import (
    Kafka,
    Materialized,
    SchemaRegistry,
    Testdrive,
    Zookeeper,
)
from materialize.zippy.framework import Test
from materialize.zippy.kafka_actions import *
from materialize.zippy.mz_actions import *
from materialize.zippy.source_actions import *
from materialize.zippy.table_actions import *
from materialize.zippy.view_actions import *

SERVICES = [
    Zookeeper(),
    Kafka(),
    SchemaRegistry(),
    # --persistent-kafka-sources can not be enabled due to gh#11711 , gh#11506
    Materialized(options="--persistent-user-tables"),
    Testdrive(validate_data_dir=False, no_reset=True, seed=1),
]

all_action_classes = [
    MzStart,
    MzStop,
    KafkaStart,
    #   KafkaStop,
    CreateTopic,
    CreateSource,
    CreateSource,
    CreateTable,
    CreateView,
    ValidateView,
    ValidateView,
    Insert,
    ShiftForward,
    ShiftBackward,
    DeleteFromTail,
    DeleteFromHead,
    KafkaInsert,
    KafkaDeleteFromHead,
    KafkaDeleteFromTail,
]


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    c.start_and_wait_for_tcp(services=["zookeeper", "kafka", "schema-registry"])
    c.up("testdrive", persistent=True)

    random.seed(1)

    print("Generating test...")
    test = Test(action_classes=all_action_classes, max_actions=500)
    print("Running test...")
    test.run(c)
