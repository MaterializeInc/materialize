# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# This mzcompose currently tests `UPSERT` sources with `DISK` configured.
# TODO(guswynn): move ALL upsert-related tests into this directory.

from pathlib import Path

from materialize import ci_util
from materialize.mzcompose import Composition, WorkflowArgumentParser
from materialize.mzcompose.services import (
    Kafka,
    Materialized,
    SchemaRegistry,
    Testdrive,
    Zookeeper,
)

SERVICES = [
    Zookeeper(),
    Kafka(),
    SchemaRegistry(),
    Materialized(),
    Testdrive(),
]


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    """Run testdrive."""
    parser.add_argument(
        "--kafka-default-partitions",
        type=int,
        metavar="N",
        help="set the default number of kafka partitions per topic",
    )
    parser.add_argument(
        "--default-size",
        type=int,
        default=Materialized.Size.DEFAULT_SIZE,
        help="Use SIZE 'N-N' for replicas and SIZE 'N' for sources",
    )

    parser.add_argument("--replicas", type=int, default=1, help="use multiple replicas")

    parser.add_argument(
        "files",
        nargs="*",
        default=["*.td"],
        help="run against the specified files",
    )
    args = parser.parse_args()

    dependencies = ["materialized", "zookeeper", "kafka", "schema-registry"]

    testdrive = Testdrive(
        forward_buildkite_shard=True,
        kafka_default_partitions=args.kafka_default_partitions,
        validate_postgres_stash="materialized",
    )

    materialized = Materialized(
        default_size=args.default_size,
        options=[
            "--orchestrator-process-scratch-directory=/mzdata/source_data"
            "--system-var-default=upsert_source_disk_default=true"
        ],
    )

    with c.override(testdrive, materialized):
        c.up(*dependencies)

        if args.replicas > 1:
            c.sql("DROP CLUSTER default CASCADE")
            # Make sure a replica named 'r1' always exists
            replica_names = [
                "r1" if replica_id == 0 else f"replica{replica_id}"
                for replica_id in range(0, args.replicas)
            ]
            replica_string = ",".join(
                f"{replica_name} (SIZE '{materialized.default_replica_size}')"
                for replica_name in replica_names
            )
            c.sql(f"CREATE CLUSTER default REPLICAS ({replica_string})")

        try:
            junit_report = ci_util.junit_report_filename(c.name)
            c.run(
                "testdrive",
                f"--junit-report={junit_report}",
                f"--var=replicas={args.replicas}",
                f"--var=default-replica-size={materialized.default_replica_size}",
                f"--var=default-storage-size={materialized.default_storage_size}",
                *args.files,
            )
        finally:
            ci_util.upload_junit_report(
                "testdrive", Path(__file__).parent / junit_report
            )
