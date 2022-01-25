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
    Redpanda,
    SchemaRegistry,
    Squid,
    Testdrive,
    Zookeeper,
)

SERVICES = [
    Zookeeper(),
    Kafka(),
    SchemaRegistry(),
    Redpanda(),
    Squid(),
    Localstack(),
    Materialized(),
    Testdrive(volumes_extra=["../testdrive:/workdir/testdrive"]),
]


@dataclass
class TestCase:
    name: str
    env: List[str]
    files: List[str]


test_cases = [
    TestCase(
        name="with-proxy",
        env=["ALL_PROXY=http://squid:3128"],
        files=["testdrive/avro-registry.td", "testdrive/esoteric/s3.td"],
    ),
    TestCase(
        name="proxy-failure",
        env=["ALL_PROXY=http://squid:3128"],
        files=["testdrive/avro-registry.td", "testdrive/esoteric/s3.td"],
    ),
    TestCase(
        name="no-proxy",
        env=[
            "ALL_PROXY=http://localhost:1234",
            "NO_PROXY=schema-registry,amazonaws.com,localstack",
        ],
        files=["testdrive/avro-registry.td", "testdrive/esoteric/s3.td"],
    ),
]


def workflow_proxy(c: Composition, parser: WorkflowArgumentParser) -> None:
    """Run the proxy tests."""
    parser.add_argument(
        "--redpanda",
        action="store_true",
        help="run against Redpanda instead of the Confluent Platform",
    )
    parser.add_argument(
        "--aws-region",
        help="run against the specified AWS region instead of localstack",
    )
    args = parser.parse_args()

    dependencies = ["squid"]
    if args.redpanda:
        dependencies += ["redpanda"]
    else:
        dependencies += ["zookeeper", "kafka", "schema-registry"]
    if not args.aws_region:
        dependencies += ["localstack"]
    c.start_and_wait_for_tcp(dependencies)

    aws_arg = (
        f"--aws-region={args.aws_region}"
        if args.aws_region
        else "--aws-endpoint=http://localstack:4566"
    )

    for test_case in test_cases:
        print(f"Running test case {test_case.name!r}")
        with c.override(Materialized(environment_extra=test_case.env)):
            c.up("materialized")
            c.wait_for_materialized("materialized")
            c.run("testdrive-svc", aws_arg, *test_case.files)
