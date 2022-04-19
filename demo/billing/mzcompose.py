# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize.mzcompose import Composition, WorkflowArgumentParser


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    parser.add_argument("--message-count", type=int, default=1000)
    parser.add_argument("--partitions", type=int, default=1)
    parser.add_argument("--check-sink", action="store_true")
    args = parser.parse_args()

    c.start_and_wait_for_tcp(services=["zookeeper", "redpanda"])
    c.run(
        "billing-demo",
        "--materialized-host=materialized",
        "--kafka-host=kafka",
        "--schema-registry-url=http://schema-registry:8081",
        "--csv-file-name=/share/billing-demo/data/prices.csv",
        "--create-topic",
        "--replication-factor=1",
        f"--message-count={args.message_count}",
        f"--partitions={args.partitions}",
        *(["--check-sink"] if args.check_sink else []),
    )
