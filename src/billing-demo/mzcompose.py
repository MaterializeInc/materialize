# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize.mzcompose import Composition, Service, WorkflowArgumentParser
from materialize.mzcompose.services import (
    Kafka,
    Materialized,
    Redpanda,
    SchemaRegistry,
    Zookeeper,
)

SERVICES = [
    Kafka(),
    SchemaRegistry(),
    Zookeeper(),
    Redpanda(),
    Materialized(),
    Service(
        name="billing-demo",
        config={
            "mzbuild": "billing-demo",
        },
    ),
]


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    parser.add_argument("--message-count", type=int, default=1000)
    parser.add_argument("--partitions", type=int, default=1)
    parser.add_argument("--check-sink", action="store_true")
    parser.add_argument(
        "--redpanda",
        action="store_true",
        help="run against Redpanda instead of the Confluent Platform",
    )
    args = parser.parse_args()

    dependencies = ["materialized"]
    if args.redpanda:
        dependencies += ["redpanda"]
    else:
        dependencies += ["zookeeper", "kafka", "schema-registry"]

    c.start_and_wait_for_tcp(services=dependencies)
    c.run(
        "billing-demo",
        "--materialized-host=materialized",
        "--kafka-host=kafka",
        "--schema-registry-url=http://schema-registry:8081",
        "--create-topic",
        "--replication-factor=1",
        f"--message-count={args.message_count}",
        f"--partitions={args.partitions}",
        *(["--check-sink"] if args.check_sink else []),
    )
