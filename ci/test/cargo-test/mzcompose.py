# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
import os

import requests

from materialize import ROOT, spawn
from materialize.mzcompose import Composition, Service
from materialize.mzcompose.services import Kafka, SchemaRegistry, Zookeeper

SERVICES = [
    Zookeeper(),
    Kafka(),
    SchemaRegistry(),
    Service(
        name="ci-cargo-test",
        config={
            "mzbuild": "ci-cargo-test",
            "environment": [
                "ZOOKEEPER_ADDR=zookeeper:2181",
                "KAFKA_ADDRS=kafka:9092",
                "SCHEMA_REGISTRY_URL=http://schema-registry:8081",
                "MZ_SOFT_ASSERTIONS=1",
                "MZ_PERSIST_EXTERNAL_STORAGE_TEST_S3_BUCKET=mtlz-test-persist-1d-lifecycle-delete",
                "AWS_DEFAULT_REGION",
                "AWS_ACCESS_KEY_ID",
                "AWS_SECRET_ACCESS_KEY",
                "AWS_SESSION_TOKEN",
            ],
            "volumes": ["../../../:/workdir"],
        },
    ),
]


def workflow_default(c: Composition) -> None:
    c.start_and_wait_for_tcp(["zookeeper", "kafka", "schema-registry"])
    c.run("ci-cargo-test", "run-tests")
    token = os.environ["BUILDKITE_TEST_ANALYTICS_API_KEY_CARGO_TEST"]
    if len(token) < 1:
        print("Analytics API key empty, skipping junit reporting")
        return
    with open(f"{ROOT.as_posix()}/results.json") as f:
        junit_xml = spawn.capture(args=["cargo2junit"], stdin=f)
        requests.post(
            "https://analytics-api.buildkite.com/v1/uploads",
            headers={"Authorization": f"Token {token}"},
            json={
                "format": "junit",
                "run_env": {
                    "key": os.environ["BUILDKITE_BUILD_ID"],
                    "CI": "buildkite",
                    "number": os.environ["BUILDKITE_BUILD_NUMBER"],
                    "job_id": os.environ["BUILDKITE_JOB_ID"],
                    "branch": os.environ["BUILDKITE_BRANCH"],
                    "commit_sha": os.environ["BUILDKITE_COMMIT"],
                    "message": os.environ["BUILDKITE_MESSAGE"],
                    "url": os.environ["BUILDKITE_BUILD_URL"],
                },
                "data": junit_xml,
            },
        )
