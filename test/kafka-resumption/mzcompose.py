# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Test that the Kafka <-> Materialize connection (source + sink) can survive
network problems and interruptions using Toxiyproxy.
"""

import argparse
import random

from materialize.mzcompose.composition import Composition, WorkflowArgumentParser
from materialize.mzcompose.services.clusterd import Clusterd
from materialize.mzcompose.services.kafka import Kafka
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.mz import Mz
from materialize.mzcompose.services.redpanda import Redpanda
from materialize.mzcompose.services.schema_registry import SchemaRegistry
from materialize.mzcompose.services.testdrive import Testdrive
from materialize.mzcompose.services.toxiproxy import Toxiproxy
from materialize.mzcompose.services.zookeeper import Zookeeper

SERVICES = [
    Zookeeper(),
    Kafka(),
    SchemaRegistry(),
    Redpanda(),
    Mz(app_password=""),
    Materialized(default_replication_factor=2),
    Clusterd(),
    Toxiproxy(),
    Testdrive(default_timeout="120s"),
]


def parse_args(parser: WorkflowArgumentParser) -> argparse.Namespace:
    parser.add_argument(
        "--redpanda",
        action="store_true",
        help="run against Redpanda instead of the Confluent Platform",
    )

    return parser.parse_args()


def get_kafka_services(redpanda: bool) -> list[str]:
    return ["redpanda"] if redpanda else ["zookeeper", "kafka", "schema-registry"]


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    def process(name: str) -> None:
        if name == "default":
            return
        with c.test_case(name):
            c.workflow(name, *parser.args)

    c.test_parts(list(c.workflows.keys()), process)


#
# Test the kafka sink resumption logic in the presence of networking problems
#
def workflow_sink_networking(c: Composition, parser: WorkflowArgumentParser) -> None:
    args = parse_args(parser)
    c.up(*(["materialized", "toxiproxy"] + get_kafka_services(args.redpanda)))

    seed = random.getrandbits(16)
    for i, failure_mode in enumerate(
        [
            "toxiproxy-close-connection.td",
            "toxiproxy-limit-connection.td",
            "toxiproxy-timeout.td",
            "toxiproxy-timeout-hold.td",
        ]
    ):
        c.up("toxiproxy")
        c.run_testdrive_files(
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


def workflow_sink_kafka_restart(c: Composition, parser: WorkflowArgumentParser) -> None:
    args = parse_args(parser)

    # Sleeping for 5s before the transaction commits virtually guarantees that
    # there will be an open transaction when the Kafka/Redpanda service is
    # killed, which lets us tests whether open transactions for the same
    # producer ID are properly aborted after a broker restart.
    with c.override(
        Materialized(
            environment_extra=["FAILPOINTS=kafka_sink_commit_transaction=sleep(5000)"],
            default_replication_factor=2,
        )
    ):
        c.up(*(["materialized"] + get_kafka_services(args.redpanda)))

        seed = random.getrandbits(16)
        c.run_testdrive_files(
            "--no-reset",
            "--max-errors=1",
            f"--seed={seed}",
            f"--temp-dir=/share/tmp/kafka-resumption-{seed}",
            "--default-timeout=40s",
            "sink-kafka-restart/setup.td",
        )
        c.kill("redpanda" if args.redpanda else "kafka")
        c.up("redpanda" if args.redpanda else "kafka")
        c.run_testdrive_files(
            "--no-reset",
            "--max-errors=1",
            f"--seed={seed}",
            f"--temp-dir=/share/tmp/kafka-resumption-{seed}",
            "--default-timeout=40s",
            "sink-kafka-restart/verify.td",
            "sink-kafka-restart/cleanup.td",
        )


def workflow_source_resumption(c: Composition, parser: WorkflowArgumentParser) -> None:
    """Test creating sources in a remote clusterd process."""
    args = parse_args(parser)

    with c.override(
        Testdrive(no_reset=True, consistent_seed=True),
        Clusterd(workers=4),
    ):
        c.up(*(["materialized", "clusterd"] + get_kafka_services(args.redpanda)))

        c.run_testdrive_files("source-resumption/setup.td")
        c.run_testdrive_files("source-resumption/verify.td")

        c.kill("clusterd")
        c.up("clusterd")
        c.sleep(10)

        # Verify the same data is query-able, and that we can make forward progress
        c.run_testdrive_files("source-resumption/verify.td")
        c.run_testdrive_files("source-resumption/re-ingest-and-verify.td")


def workflow_sink_queue_full(c: Composition, parser: WorkflowArgumentParser) -> None:
    """Similar to the sink-networking workflow, but with 11 million rows (more then the 11 million defined as queue.buffering.max.messages) and only creating the sink after these rows are ingested into Mz. Triggers database-issues#7442"""
    args = parse_args(parser)
    seed = random.getrandbits(16)
    c.up(*(["materialized", "toxiproxy"] + get_kafka_services(args.redpanda)))
    c.run_testdrive_files(
        "--no-reset",
        "--max-errors=1",
        f"--seed={seed}",
        f"--temp-dir=/share/tmp/kafka-resumption-{seed}",
        "sink-queue-full/setup.td",
        "sink-queue-full/during.td",
        "sink-queue-full/verify-success.td",
        "sink-queue-full/cleanup.td",
    )
