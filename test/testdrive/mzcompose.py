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
    Localstack,
    Materialized,
    SchemaRegistry,
    Testdrive,
    Zookeeper,
)

SERVICES = [
    Zookeeper(),
    Kafka(),
    SchemaRegistry(),
    Localstack(),
    Materialized(),
    Testdrive(depends_on=["kafka", "schema-registry", "materialized"]),
]


def workflow_testdrive(c: Composition, parser: WorkflowArgumentParser) -> None:
    """Run testdrive tests."""
    parser.add_argument(
        "--aws-region",
        help="run against the specified AWS region instead of localstack",
    )
    parser.add_argument(
        "--materialized-workers",
        help="the number of materialized workers to use",
        type=int,
    ),
    parser.add_argument(
        "--materialized-persistent-user-tables",
        action="store_true",
        help="run materialized with the --persistent-user-tables option",
    )
    parser.add_argument(
        "filter",
        nargs="*",
        default=["*.td"],
        help="limit to only the files matching filter",
    )
    args = parser.parse_args()

    if not args.aws_region:
        c.up("localstack")

    materialized = Materialized(
        workers=args.materialized_workers,
        options=["--persistent-user-tables"]
        if args.materialized_persistent_user_tables
        else [],
    )

    with c.override(materialized):
        c.run(
            "testdrive-svc",
            f"--aws-region={args.aws_region}"
            if args.aws_region
            else "--aws-endpoint=http://localstack:4566",
            *args.filter,
        )
