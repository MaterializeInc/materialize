# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from typing import Any, Dict, List

from materialize.mzcompose import Composition
from materialize.mzcompose.services import (
    Kafka,
    Localstack,
    Materialized,
    SchemaRegistry,
    Squid,
    Testdrive,
    Zookeeper,
)

prerequisites = ["zookeeper", "kafka", "schema-registry", "squid", "localstack"]

# Run certain testdrive tests for each combination of env variables under test
tests: List[Dict[str, Any]] = [
    {
        "name": "with_proxy",
        "env": ["ALL_PROXY=http://squid:3128"],
        "td": ["testdrive/avro-registry.td", "testdrive/esoteric/s3.td"],
    },
    {
        "name": "proxy_failure",
        "env": ["ALL_PROXY=http://localhost:1234"],
        "td": ["proxy-failure.td"],
    },
    {
        "name": "no_proxy",
        "env": [
            "ALL_PROXY=http://localhost:1234",
            "NO_PROXY=schema-registry,amazonaws.com,localstack",
        ],
        "td": ["testdrive/avro-registry.td", "testdrive/esoteric/s3.td"],
    },
]

# Construct a dedicated Mz instance for each set of env variables under test
for t in tests:
    t["mz"] = Materialized(
        name=f"materialized_{t['name']}",
        hostname="materialized",
        environment_extra=t["env"],
    )

mzs = [t["mz"] for t in tests]

SERVICES = [
    Zookeeper(),
    Kafka(),
    SchemaRegistry(),
    Squid(),
    Localstack(),
    *mzs,
    Testdrive(volumes_extra=["../testdrive:/workdir/testdrive"]),
]


def workflow_proxy(c: Composition) -> None:
    c.start_and_wait_for_tcp(services=prerequisites)
    test_proxy(c, "--aws-endpoint=http://localstack:4566")


def workflow_proxy_ci(c: Composition) -> None:
    c.start_and_wait_for_tcp(services=prerequisites)
    test_proxy(c, "--aws-region=us-east-2")


def test_proxy(c: Composition, aws: str) -> None:
    for test in tests:
        mz: Materialized = test["mz"]
        c.up(mz.name)
        c.wait_for_materialized(mz.name)
        c.run("testdrive-svc", aws, *test["td"])
        c.kill(mz.name)
