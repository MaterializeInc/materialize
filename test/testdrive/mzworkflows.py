# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import os

from materialize.mzcompose import Workflow
from materialize.mzcompose.services import (
    Kafka,
    Localstack,
    Materialized,
    SchemaRegistry,
    Testdrive,
    Zookeeper,
)

mz_default = Materialized(name="mz_default", hostname="materialized")
mz_workers_1 = Materialized(name="mz_workers_1", hostname="materialized", workers=1)
mz_workers_32 = Materialized(name="mz_workers_32", hostname="materialized", workers=32)
mz_persistence = Materialized(
    name="mz_persistence", hostname="materialized", options="--persistent-user-tables"
)

services = [
    Zookeeper(),
    Kafka(),
    SchemaRegistry(),
    Localstack(),
    mz_default,
    mz_workers_1,
    mz_workers_32,
    mz_persistence,
    Testdrive(),
]

tests = os.getenv("TD_TEST", "*.td esoteric/*.td")
tests_ci = tests + " esoteric/pubnub/pubnub.td"
aws_localstack = "--aws-endpoint http://localstack:4566"
aws_amazon = "--aws-region=us-east-2"


def workflow_testdrive(w: Workflow):
    """Run non-esoteric tests with localstack"""
    w.start_and_wait_for_tcp(services=["localstack"])
    test_testdrive(w, mz_default, aws_localstack, tests)


def workflow_testdrive_ci(w: Workflow):
    """Run all tests with actual AWS credentials"""
    test_testdrive(w, mz_default, aws_amazon, tests_ci)


def workflow_testdrive_ci_workers_1(w: Workflow):
    test_testdrive(w, mz_workers_1, aws_amazon, tests_ci)


def workflow_testdrive_ci_workers_32(w: Workflow):
    test_testdrive(w, mz_workers_32, aws_amazon, tests_ci)


def workflow_persistence_testdrive(w: Workflow):
    test_testdrive(w, mz_persistence, aws_amazon, tests_ci)


def test_testdrive(w: Workflow, mz: Materialized, aws: str, tests: str):
    w.start_and_wait_for_tcp(
        services=["zookeeper", "kafka", "schema-registry", mz.name]
    )
    w.wait_for_mz(service=mz.name)
    w.run_service(service="testdrive-svc", command=f"{aws} {tests}")
    w.kill_services(services=[mz.name])
