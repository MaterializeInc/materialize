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

#
# Test the kafka sink resumption logic
#
def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
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
            # TODO: Enable https://github.com/MaterializeInc/materialize/issues/11085
            # "toxiproxy-timeout-hold.td",
        ]
    ):
        c.start_and_wait_for_tcp(["toxiproxy"])
        c.run(
            "testdrive",
            "--no-reset",
            "--max-errors=1",
            f"--seed={seed}{i}",
            f"--temp-dir=/share/tmp/kafka-resumption-{seed}{i}",
            "setup.td",
            failure_mode,
            "during.td",
            "sleep.td",
            "toxiproxy-restore-connection.td",
            "verify-success.td",
            "cleanup.td",
        )
        c.kill("toxiproxy")
