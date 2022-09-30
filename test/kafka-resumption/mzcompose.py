# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import random

from materialize.mzcompose import Composition, WorkflowArgumentParser
from materialize.mzcompose.services import (
    Kafka,
    Materialized,
    SchemaRegistry,
    Testdrive,
    Toxiproxy,
    Zookeeper,
)

SERVICES = [
    Zookeeper(),
    Kafka(),
    SchemaRegistry(),
    Materialized(),
    Toxiproxy(),
    Testdrive(default_timeout="120s"),
]

def workflow_default(c: Composition) -> None:
    c.workflow("sink-networking")

#
# Test the kafka sink resumption logic in the presence of networking problems
#
def workflow_sink_networking(c: Composition) -> None:
    c.start_and_wait_for_tcp(
        services=["zookeeper", "kafka", "schema-registry", "materialized", "toxiproxy"]
    )
    c.wait_for_materialized()

    seed = random.getrandbits(16)
    for i, failure_mode in enumerate(
        [
            "toxiproxy-close-connection.td",
            "toxiproxy-limit-connection.td",
            "toxiproxy-timeout.td",
            "toxiproxy-timeout-hold.td",
        ]
    ):
        c.start_and_wait_for_tcp(["toxiproxy"])
        c.run(
            "testdrive",
            "--no-reset",
            "--max-errors=1",
            f"--seed={seed}{i}",
            f"--temp-dir=/share/tmp/kafka-resumption-{seed}{i}",
            "sink-networking/setup.td",
            f"sink-networking/{failure_mode}",
            "sink-networking/during.td",
            "sink-networking/sleep.td",
            "sink-networking/toxiproxy-restore-connection.td",
            "sink-networking/verify-success.td",
            "sink-networking/cleanup.td",
        )
        c.kill("toxiproxy")
