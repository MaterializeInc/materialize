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

SERVICES = [
    Materialized(),
    Testdrive(),
    Zookeeper(),
    Kafka(),
    SchemaRegistry(),
]


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    """Run testdrive against various sources to validate expected metrics behavior"""
    parser.add_argument(
        "files",
        nargs="*",
        default=["*.td"],
        help="run against the specified files",
    )

    args = parser.parse_args()
    c.start_and_wait_for_tcp(
        services=[
            "zookeeper",
            "kafka",
            "schema-registry",
            "materialized",
        ]
    )
    c.run("testdrive-svc", *args.files)
