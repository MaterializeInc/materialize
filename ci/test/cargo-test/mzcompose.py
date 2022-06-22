# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize import ROOT, ci_util, spawn
from materialize.mzcompose import Composition, Service
from materialize.mzcompose.services import Kafka, Postgres, SchemaRegistry, Zookeeper

SERVICES = [
    Zookeeper(),
    Kafka(),
    SchemaRegistry(),
    Postgres(image="postgres:14.2"),
    Service(
        name="ci-cargo-test",
        config={
            "mzbuild": "ci-cargo-test",
            "environment": [
                "ZOOKEEPER_ADDR=zookeeper:2181",
                "KAFKA_ADDRS=kafka:9092",
                "SCHEMA_REGISTRY_URL=http://schema-registry:8081",
                "POSTGRES_URL=postgres://postgres:postgres@postgres",
                "MZ_SOFT_ASSERTIONS=1",
                "MZ_PERSIST_EXTERNAL_STORAGE_TEST_S3_BUCKET=mtlz-test-persist-1d-lifecycle-delete",
                "MZ_PERSIST_EXTERNAL_STORAGE_TEST_POSTGRES_URL=postgres://postgres:postgres@postgres",
                "AWS_DEFAULT_REGION",
                "AWS_ACCESS_KEY_ID",
                "AWS_SECRET_ACCESS_KEY",
                "AWS_SESSION_TOKEN",
            ],
            "volumes": ["../../../:/workdir"],
            "ulimits": {
                "core": 0,
            },
        },
    ),
]


def workflow_default(c: Composition) -> None:
    c.start_and_wait_for_tcp(["zookeeper", "kafka", "schema-registry", "postgres"])
    try:
        c.run("ci-cargo-test", "run-tests")
    finally:
        junit_report = ci_util.junit_report_filename("cargo-test")
        spawn.runv(
            ["cargo2junit"],
            stdin=(ROOT / "results.json").open("rb"),
            stdout=junit_report.open("wb"),
        ),
        ci_util.upload_junit_report("cargo-test", junit_report)
