# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import random
from os import environ
from unittest.mock import patch

from materialize.mzcompose import Composition, WorkflowArgumentParser
from materialize.mzcompose.services import (
    Kafka,
    Materialized,
    SchemaRegistry,
    Testdrive,
    Toxiproxy,
    Zookeeper,
)

# Pick a non-default port to make sure nothing is accidentally going around the proxy
KAFKA_SINK_PORT = 9091
SERVICES = [
    Zookeeper(),
    Kafka(port=KAFKA_SINK_PORT),
    SchemaRegistry(kafka_servers=[("kafka", f"{KAFKA_SINK_PORT}")]),
    Materialized(),
    Toxiproxy(),
    Testdrive(kafka_url="toxiproxy:9093"),
]

#
# Test the kafka sink resumption logic
#
def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    parser.add_argument(
        "--seed",
        help="an alternate seed to use to avoid clashing with existing topics",
        type=int,
        default=int(random.getrandbits(16)),
    )
    args = parser.parse_args()

    c.start_and_wait_for_tcp(
        services=["zookeeper", "kafka", "schema-registry", "materialized", "toxiproxy"]
    )
    c.wait_for_materialized()

    for i, failure_mode in enumerate(
        [
            "toxiproxy-close-connection.td",
            "toxiproxy-timeout.td",
        ]
    ):
        c.run(
            "testdrive-svc",
            "--no-reset",
            "--max-errors=1",
            f"--seed={args.seed}{i}",
            f"--temp-dir=/share/tmp/kafka-resumption-{args.seed}-{i}",
            "setup.td",
            failure_mode,
            "during.td",
            "sleep.td",
            "toxiproxy-restore-connection.td",
            "verify-success.td",
            "cleanup.td",
        )
