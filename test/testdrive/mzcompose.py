# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from pathlib import Path

from materialize import ci_util, spawn, ui
from materialize.mzcompose import Composition, WorkflowArgumentParser
from materialize.mzcompose.services import (
    Kafka,
    Localstack,
    Materialized,
    Redpanda,
    SchemaRegistry,
    Testdrive,
    Zookeeper,
)
from materialize.xcompile import Arch

SERVICES = [
    Zookeeper(),
    Kafka(),
    SchemaRegistry(),
    Redpanda(),
    Localstack(),
    Materialized(),
    Testdrive(),
]


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    """Run testdrive."""
    parser.add_argument(
        "--redpanda",
        action="store_true",
        help="run against Redpanda instead of the Confluent Platform",
    )
    parser.add_argument(
        "--aws-region",
        help="run against the specified AWS region instead of localstack",
    )
    parser.add_argument(
        "--workers",
        type=int,
        metavar="N",
        help="set the number of materialized dataflow workers",
    )
    parser.add_argument(
        "--kafka-default-partitions",
        type=int,
        metavar="N",
        help="set the default number of kafka partitions per topic",
    )
    parser.add_argument(
        "--persistent-user-tables",
        action="store_true",
        help="enable the --persistent-user-tables materialized option",
    )
    parser.add_argument(
        "files",
        nargs="*",
        default=["*.td", "esoteric/*.td"],
        help="run against the specified files",
    )
    args = parser.parse_args()

    if not args.redpanda and Arch.host() == Arch.AARCH64:
        ui.warn(
            "Running the Confluent Platform in Docker on ARM-based machines is "
            "nearly unusably slow. Consider using Redpanda instead (--redpanda) "
            "or running tests without mzcompose."
        )

    dependencies = ["materialized"]
    if args.redpanda:
        dependencies += ["redpanda"]
    else:
        dependencies += ["zookeeper", "kafka", "schema-registry"]

    materialized = Materialized(
        workers=args.workers,
        options=["--persistent-user-tables"] if args.persistent_user_tables else [],
    )

    testdrive = Testdrive(
        forward_buildkite_shard=True,
        kafka_default_partitions=args.kafka_default_partitions,
        entrypoint_extra=[f"--aws-region={args.aws_region}"]
        if args.aws_region
        else ["--aws-endpoint=http://localstack:4566"],
    )

    with c.override(materialized, testdrive):
        c.start_and_wait_for_tcp(services=dependencies)
        c.wait_for_materialized("materialized")
        try:
            junit_report = ci_util.junit_report_filename(c.name)
            c.run("testdrive-svc", f"--junit-report={junit_report}", *args.files)
        finally:
            ci_util.upload_junit_report(
                "testdrive", Path(__file__).parent / junit_report
            )


def workflow_testdrive_redpanda_ci(c: Composition) -> None:
    """Run testdrive against files known to be supported by Redpanda."""

    # https://github.com/vectorizedio/redpanda/issues/2397
    KNOWN_FAILURES = {"kafka-time-offset.td"}

    files = set(
        # NOTE(benesch): invoking the shell like this to filter testdrive files is
        # pretty gross. Let's not get into the habit of using this construction.
        spawn.capture(
            ["sh", "-c", "grep -lr '\$.*kafka-ingest' *.td"], cwd=Path(__file__).parent
        ).split()
    )
    files -= KNOWN_FAILURES
    c.workflow("default", "--redpanda", *files)
