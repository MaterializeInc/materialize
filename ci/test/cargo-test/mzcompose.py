# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import os

from materialize import MZ_ROOT, buildkite, spawn, ui
from materialize.mzcompose.composition import Composition, WorkflowArgumentParser
from materialize.mzcompose.services.cockroach import Cockroach
from materialize.mzcompose.services.kafka import Kafka
from materialize.mzcompose.services.postgres import Postgres
from materialize.mzcompose.services.schema_registry import SchemaRegistry
from materialize.mzcompose.services.zookeeper import Zookeeper

SERVICES = [
    Zookeeper(),
    Kafka(
        # We need a stable port to advertise, so pick one that is unlikely to
        # conflict with a Kafka cluster running on the local machine.
        ports=["30123:30123"],
        allow_host_ports=True,
        environment_extra=[
            "KAFKA_ADVERTISED_LISTENERS=HOST://localhost:30123,PLAINTEXT://kafka:9092",
            "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=HOST:PLAINTEXT,PLAINTEXT:PLAINTEXT",
        ],
    ),
    SchemaRegistry(),
    Postgres(image="postgres:14.2"),
    Cockroach(),
]


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    parser.add_argument("--miri-full", action="store_true")
    parser.add_argument("--miri-fast", action="store_true")
    parser.add_argument("args", nargs="*")
    args = parser.parse_args()
    c.up("zookeeper", "kafka", "schema-registry", "postgres", "cockroach")
    # Heads up: this intentionally runs on the host rather than in a Docker
    # image. See #13010.
    postgres_url = (
        f"postgres://postgres:postgres@localhost:{c.default_port('postgres')}"
    )
    cockroach_url = f"postgres://root@localhost:{c.default_port('cockroach')}"

    env = dict(
        os.environ,
        ZOOKEEPER_ADDR=f"localhost:{c.default_port('zookeeper')}",
        KAFKA_ADDRS="localhost:30123",
        SCHEMA_REGISTRY_URL=f"http://localhost:{c.default_port('schema-registry')}",
        POSTGRES_URL=postgres_url,
        COCKROACH_URL=cockroach_url,
        MZ_SOFT_ASSERTIONS="1",
        MZ_PERSIST_EXTERNAL_STORAGE_TEST_S3_BUCKET="mz-test-persist-1d-lifecycle-delete",
        MZ_S3_UPLOADER_TEST_S3_BUCKET="mz-test-1d-lifecycle-delete",
        MZ_PERSIST_EXTERNAL_STORAGE_TEST_POSTGRES_URL=cockroach_url,
    )

    coverage = ui.env_is_truthy("CI_COVERAGE_ENABLED")

    if coverage:
        # TODO(def-): For coverage inside of clusterd called from unit tests need
        # to set LLVM_PROFILE_FILE in test code invoking clusterd and later
        # aggregate the data.
        (MZ_ROOT / "coverage").mkdir(exist_ok=True)
        env["CARGO_LLVM_COV_SETUP"] = "no"
        # There is no pure build command in cargo-llvm-cov, so run with
        # --version as a workaround.
        spawn.runv(
            [
                "cargo",
                "llvm-cov",
                "run",
                "--bin",
                "clusterd",
                "--release",
                "--no-report",
                "--",
                "--version",
            ],
            env=env,
        )

        cmd = [
            "cargo",
            "llvm-cov",
            "nextest",
            "--release",
            "--no-clean",
            "--workspace",
            "--lcov",
            "--output-path",
            "coverage/cargotest.lcov",
            "--profile=coverage",
            # We still want a coverage report on crash
            "--ignore-run-fail",
        ]
        try:
            spawn.runv(cmd + args.args, env=env)
        finally:
            spawn.runv(["zstd", "coverage/cargotest.lcov"])
            buildkite.upload_artifact("coverage/cargotest.lcov.zst")
    else:
        if args.miri_full:
            spawn.runv(
                ["bin/ci-builder", "run", "nightly", "ci/test/cargo-test-miri.sh"],
                env=env,
            )
        elif args.miri_fast:
            spawn.runv(
                ["bin/ci-builder", "run", "nightly", "ci/test/cargo-test-miri-fast.sh"],
                env=env,
            )
        else:
            spawn.runv(
                [
                    "cargo",
                    "build",
                    "--workspace",
                    "--bin",
                    "clusterd",
                    "--profile=ci",
                ],
                env=env,
            )

            cpu_count = os.cpu_count()
            assert cpu_count

            partition = buildkite.get_parallelism_index() + 1
            total = buildkite.get_parallelism_count()

            spawn.runv(
                [
                    "cargo",
                    "nextest",
                    "run",
                    "--workspace",
                    "--all-features",
                    "--profile=ci",
                    "--cargo-profile=ci",
                    f"--partition=count:{partition}/{total}",
                    # Most tests don't use 100% of a CPU core, so run two tests per CPU.
                    # TODO(def-): Reenable when #19931 is fixed
                    # f"--test-threads={cpu_count * 2}",
                    *args.args,
                ],
                env=env,
            )
