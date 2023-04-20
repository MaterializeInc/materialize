# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import os

from materialize import spawn
from materialize.mzcompose import Composition, WorkflowArgumentParser
from materialize.mzcompose.services import (
    Cockroach,
    Kafka,
    Postgres,
    SchemaRegistry,
    Zookeeper,
)

SERVICES = [
    Zookeeper(),
    Kafka(
        # We need a stable port to advertise, so pick one that is unlikely to
        # conflict with a Kafka cluster running on the local machine.
        port="30123:30123",
        allow_host_ports=True,
        extra_environment=[
            "KAFKA_ADVERTISED_LISTENERS=HOST://localhost:30123,PLAINTEXT://kafka:9092",
            "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=HOST:PLAINTEXT,PLAINTEXT:PLAINTEXT",
        ],
    ),
    SchemaRegistry(),
    Postgres(image="postgres:14.2"),
    Cockroach(),
]


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    parser.add_argument("args", nargs="*")
    args = parser.parse_args()
    c.up("zookeeper", "kafka", "schema-registry", "postgres", "cockroach")
    # Heads up: this intentionally runs on the host rather than in a Docker
    # image. See #13010.
    postgres_url = (
        f"postgres://postgres:postgres@localhost:{c.default_port('postgres')}"
    )
    cockroach_url = f"postgres://root@localhost:{c.default_port('cockroach')}"
    spawn.runv(
        [
            "cargo",
            "build",
            "--bin",
            "clusterd",
        ]
    )
    spawn.runv(
        ["cargo", "nextest", "run", "--profile=ci", *args.args],
        env=dict(
            os.environ,
            ZOOKEEPER_ADDR=f"localhost:{c.default_port('zookeeper')}",
            KAFKA_ADDRS="localhost:30123",
            SCHEMA_REGISTRY_URL=f"http://localhost:{c.default_port('schema-registry')}",
            POSTGRES_URL=postgres_url,
            COCKROACH_URL=cockroach_url,
            MZ_SOFT_ASSERTIONS="1",
            MZ_PERSIST_EXTERNAL_STORAGE_TEST_S3_BUCKET="mtlz-test-persist-1d-lifecycle-delete",
            MZ_PERSIST_EXTERNAL_STORAGE_TEST_POSTGRES_URL=cockroach_url,
        ),
    )
