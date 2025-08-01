# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Runs the Rust-based unit tests in Debug mode.
"""

import json
import multiprocessing
import os
import shutil
import subprocess

from materialize import MZ_ROOT, buildkite, rustc_flags, spawn, ui
from materialize.cli.run import SANITIZER_TARGET
from materialize.mzcompose.composition import Composition, WorkflowArgumentParser
from materialize.mzcompose.services.azurite import Azurite
from materialize.mzcompose.services.clusterd import Clusterd
from materialize.mzcompose.services.kafka import Kafka
from materialize.mzcompose.services.minio import Minio
from materialize.mzcompose.services.postgres import (
    CockroachOrPostgresMetadata,
    Postgres,
)
from materialize.mzcompose.services.schema_registry import SchemaRegistry
from materialize.mzcompose.services.zookeeper import Zookeeper
from materialize.rustc_flags import Sanitizer
from materialize.util import PropagatingThread
from materialize.xcompile import Arch, target

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
    CockroachOrPostgresMetadata(),
    Minio(
        # We need a stable port exposed to the host since we can't pass any arguments
        # to the .pt files used in the tests.
        ports=["40109:9000", "40110:9001"],
        allow_host_ports=True,
        additional_directories=["copytos3"],
    ),
    Azurite(
        ports=["40111:10000"],
        allow_host_ports=True,
    ),
    Clusterd(),  # Only to attempt to download the binary
]


def flatten(xss):
    return [x for xs in xss for x in xs]


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    parser.add_argument("--miri-full", action="store_true")
    parser.add_argument("--miri-fast", action="store_true")
    parser.add_argument("args", nargs="*")
    args = parser.parse_args()
    c.up(
        "zookeeper",
        "kafka",
        "schema-registry",
        "postgres",
        c.metadata_store(),
        "minio",
        "azurite",
    )
    # Heads up: this intentionally runs on the host rather than in a Docker
    # image. See database-issues#3739.
    postgres_url = (
        f"postgres://postgres:postgres@localhost:{c.default_port('postgres')}"
    )
    metadata_backend_url = (
        f"postgres://root@localhost:{c.default_port(c.metadata_store())}"
    )

    env = dict(
        os.environ,
        ZOOKEEPER_ADDR=f"localhost:{c.default_port('zookeeper')}",
        KAFKA_ADDRS="localhost:30123",
        SCHEMA_REGISTRY_URL=f"http://localhost:{c.default_port('schema-registry')}",
        POSTGRES_URL=postgres_url,
        METADATA_BACKEND_URL=metadata_backend_url,
        MZ_SOFT_ASSERTIONS="1",
        MZ_PERSIST_EXTERNAL_STORAGE_TEST_S3_BUCKET="mz-test-persist-1d-lifecycle-delete",
        MZ_S3_UPLOADER_TEST_S3_BUCKET="mz-test-1d-lifecycle-delete",
        MZ_PERSIST_EXTERNAL_STORAGE_TEST_AZURE_CONTAINER="mz-test-azure",
        MZ_PERSIST_EXTERNAL_STORAGE_TEST_POSTGRES_URL=metadata_backend_url,
    )

    coverage = ui.env_is_truthy("CI_COVERAGE_ENABLED")
    sanitizer = Sanitizer[os.getenv("CI_SANITIZER", "none")]
    extra_env = {}
    clusterd_thread: PropagatingThread | None = None

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
                [
                    "bin/ci-builder",
                    "run",
                    "nightly",
                    "ci/test/cargo-test-miri.sh",
                ],
                env=env,
            )
        elif args.miri_fast:
            spawn.runv(
                [
                    "bin/ci-builder",
                    "run",
                    "nightly",
                    "ci/test/cargo-test-miri-fast.sh",
                ],
                env=env,
            )
        else:
            if sanitizer != Sanitizer.none:
                cflags = [
                    f"--target={target(Arch.host())}",
                    f"--gcc-toolchain=/opt/x-tools/{target(Arch.host())}/",
                    f"--sysroot=/opt/x-tools/{target(Arch.host())}/{target(Arch.host())}/sysroot",
                ] + rustc_flags.sanitizer_cflags[sanitizer]
                ldflags = cflags + [
                    "-fuse-ld=lld",
                    f"-L/opt/x-tools/{target(Arch.host())}/{target(Arch.host())}/lib64",
                ]
                extra_env = {
                    "CFLAGS": " ".join(cflags),
                    "CXXFLAGS": " ".join(cflags),
                    "LDFLAGS": " ".join(ldflags),
                    "CXXSTDLIB": "stdc++",
                    "CC": "cc",
                    "CXX": "c++",
                    "CPP": "clang-cpp-18",
                    "CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER": "cc",
                    "CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_LINKER": "cc",
                    "PATH": f"/sanshim:/opt/x-tools/{target(Arch.host())}/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
                    "RUSTFLAGS": (
                        env.get("RUSTFLAGS", "")
                        + " "
                        + " ".join(rustc_flags.sanitizer[sanitizer])
                    ),
                    "TSAN_OPTIONS": "report_bugs=0",  # build-scripts fail
                }
                spawn.runv(
                    [
                        "bin/ci-builder",
                        "run",
                        "nightly",
                        *flatten(
                            [
                                ["--env", f"{key}={val}"]
                                for key, val in extra_env.items()
                            ]
                        ),
                        "cargo",
                        "build",
                        "--workspace",
                        "--no-default-features",
                        "--bin",
                        "clusterd",
                        "-Zbuild-std",
                        "--target",
                        SANITIZER_TARGET,
                        "--profile=ci",
                    ],
                )
            else:
                assert (
                    buildkite.get_parallelism_count() <= 2
                ), "Special handling of parallelism, only 1 and 2 supported"
                if (
                    buildkite.get_parallelism_count() == 1
                    or buildkite.get_parallelism_index() == 0
                ):

                    def worker() -> None:
                        clusterd = c.compose["services"]["clusterd"]
                        try:
                            subprocess.run(
                                ["docker", "pull", clusterd["image"]],
                                check=True,
                                capture_output=True,
                            )
                            container_id = subprocess.check_output(
                                ["docker", "create", clusterd["image"]], text=True
                            ).strip()
                            target_dir = os.getenv("CARGO_TARGET_DIR", "target") + "/ci"
                            os.makedirs(target_dir, exist_ok=True)
                            subprocess.run(
                                [
                                    "docker",
                                    "cp",
                                    f"{container_id}:/usr/local/bin/clusterd",
                                    target_dir,
                                ],
                                check=True,
                            )
                        except subprocess.CalledProcessError as e:
                            print(f"Failed to get clusterd image: {e}")
                            spawn.runv(
                                [
                                    "cargo",
                                    "build",
                                    "--workspace",
                                    "--bin",
                                    "clusterd",
                                    "--profile=ci",
                                ],
                                env={**env, "CARGO_TARGET_DIR": "target/ci-clusterd"},
                            )
                            shutil.copy(
                                "target/ci-clusterd/ci/clusterd",
                                os.getenv("CARGO_TARGET_DIR", "target") + "/ci/",
                            )

                    clusterd_thread = PropagatingThread(target=worker)
                    clusterd_thread.start()
                    spawn.runv(
                        [
                            "cargo",
                            "nextest",
                            "run",
                            "--no-run",
                            "--all-features",
                            "--cargo-profile=ci",
                            "--profile=ci",
                            *(
                                ["--package=mz-environmentd", "--package=mz-balancerd"]
                                if buildkite.get_parallelism_count() == 2
                                else ["--workspace"]
                            ),
                        ],
                        env=env,
                    )
                    clusterd_thread.join()

            metadata = json.loads(
                subprocess.check_output(
                    ["cargo", "metadata", "--no-deps", "--format-version=1"]
                )
            )
            if sanitizer != Sanitizer.none:
                # Can't just use --workspace because of https://github.com/rust-lang/cargo/issues/7160
                for pkg in metadata["packages"]:
                    try:
                        spawn.runv(
                            [
                                "bin/ci-builder",
                                "run",
                                "nightly",
                                *flatten(
                                    [
                                        ["--env", f"{key}={val}"]
                                        for key, val in extra_env.items()
                                    ]
                                ),
                                "cargo",
                                "nextest",
                                "run",
                                "--package",
                                pkg["name"],
                                "--no-default-features",
                                "--profile=sanitizer",
                                "--cargo-profile=ci",
                                # We want all tests to run
                                "--no-fail-fast",
                                "-Zbuild-std",
                                "--target",
                                SANITIZER_TARGET,
                                *args.args,
                            ],
                            env=env,
                        )
                    except subprocess.CalledProcessError:
                        print(f"Test against package {pkg['name']} failed, continuing")

            else:
                pkgs = [
                    f"--package={p['name']}"
                    for p in metadata["packages"]
                    if p["name"] not in ("mz-environmentd", "mz-balancerd")
                ]
                spawn.runv(
                    [
                        "cargo",
                        "nextest",
                        "run",
                        # We want all tests to run
                        "--no-fail-fast",
                        "--all-features",
                        "--profile=ci",
                        "--cargo-profile=ci",
                        f"--test-threads={multiprocessing.cpu_count() * 2}",
                        *(
                            (
                                pkgs
                                if buildkite.get_parallelism_index() == 1
                                else [
                                    "--package=mz-environmentd",
                                    "--package=mz-balancerd",
                                ]
                            )
                            if buildkite.get_parallelism_count() == 2
                            else ["--workspace"]
                        ),
                        *args.args,
                    ],
                    env=env,
                )
