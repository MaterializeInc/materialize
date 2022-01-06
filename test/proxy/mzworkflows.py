# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from typing import Any, Dict, List

from materialize.mzcompose import Workflow
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
        "td": "testdrive/avro-registry.td testdrive/esoteric/s3.td",
    },
    {
        "name": "proxy_failure",
        "env": ["ALL_PROXY=http://localhost:1234"],
        "td": "proxy-failure.td",
    },
    {
        "name": "no_proxy",
        "env": [
            "ALL_PROXY=http://localhost:1234",
            "NO_PROXY=schema-registry,amazonaws.com,localstack",
        ],
        "td": "testdrive/avro-registry.td testdrive/esoteric/s3.td",
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

services = [
    Zookeeper(),
    Kafka(),
    SchemaRegistry(),
    Squid(),
    Localstack(),
    *mzs,
    Testdrive(volumes_extra=["../testdrive:/workdir/testdrive"]),
]


def workflow_proxy(w: Workflow) -> None:
    w.start_and_wait_for_tcp(services=prerequisites)
    test_proxy(w, "--aws-endpoint http://localstack:4566")


def workflow_proxy_ci(w: Workflow) -> None:
    w.start_and_wait_for_tcp(services=prerequisites)
    test_proxy(w, "--aws-region=us-east-2")


def test_proxy(w: Workflow, aws: str) -> None:
    for test in tests:
        mz: Materialized = test["mz"]
        w.start_services(services=[mz.name])
        w.wait_for_mz(service=mz.name)
        w.run_service(service="testdrive-svc", command=f"{aws} {test['td']}")
        w.kill_services(services=[mz.name])
