# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from dataclasses import dataclass
from typing import Dict, List

from materialize.mzcompose import Composition, WorkflowArgumentParser
from materialize.mzcompose.services import (
    Kafka,
    Localstack,
    Materialized,
    SchemaRegistry,
    Squid,
    Testdrive,
    Zookeeper,
)

SERVICES = [
    Zookeeper(),
    Kafka(),
    SchemaRegistry(),
    Squid(),
    Localstack(),
    Materialized(),
    Testdrive(
        volumes_extra=["../testdrive:/workdir/testdrive"],
        depends_on=["kafka", "schema-registry", "squid", "materialized"],
    ),
]


@dataclass
class Test:
    name: str
    env: List[str]
    scripts: List[str]


# Run certain testdrive tests for each combination of environment variables
# under test.
TESTS = [
    Test(
        name="with_proxy",
        env=["ALL_PROXY=http://squid:3128"],
        scripts=["testdrive/avro-registry.td", "testdrive/esoteric/s3.td"],
    ),
    Test(
        name="no_proxy",
        env=[
            "ALL_PROXY=http://localhost:1234",
            "NO_PROXY=schema-registry,amazonaws.com,localstack",
        ],
        scripts=["testdrive/avro-registry.td", "testdrive/esoteric/s3.td"],
    ),
    Test(
        name="proxy_failure",
        env=["ALL_PROXY=http://localhost:1234"],
        scripts=["proxy-failure.td"],
    ),
]


def workflow_proxy(c: Composition, parser: WorkflowArgumentParser) -> None:
    """Run testdrive tests that frob the proxy-related environment variables."""
    parser.add_argument(
        "--aws-region",
        help="run against the specified AWS region instead of localstack",
    )
    args = parser.parse_args()

    if not args.aws_region:
        c.up("localstack")

    for test in TESTS:
        print(f"Running test {test.name}")
        materialized = Materialized(environment_extra=test.env)
        with c.override(materialized):
            c.run(
                "testdrive-svc",
                f"--aws-region={args.aws_region}"
                if args.aws_region
                else "--aws-endpoint=http://localstack:4566",
                *test.scripts,
            )
