# Copyright Materialize, Inc. and contributors. All rights reserved.  #
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Tests the mz command line tool against a real Cloud instance
"""

import argparse
import json
import os
import signal
import subprocess
import threading
import time
from collections.abc import Sequence
from pathlib import Path
from textwrap import dedent
from typing import IO

import psycopg
import yaml

from materialize import MZ_ROOT, ci_util, git, spawn
from materialize.mzcompose.composition import (
    Composition,
    WorkflowArgumentParser,
)
from materialize.mzcompose.services.testdrive import Testdrive
from materialize.version_list import get_self_managed_versions

SERVICES = [
    Testdrive(),  # overridden below
]

TD_CMD = [
    "--var=default-replica-size=25cc",
    "--var=default-storage-size=25cc",
]

COMPATIBLE_TESTDRIVE_FILES = [
    "array.td",
    "cancel-subscribe.td",
    "char-varchar-distinct.td",
    "char-varchar-joins.td",
    "char-varchar-multibyte.td",
    "constants.td",
    "coordinator-multiplicities.td",
    "create-views.td",
    "date_func.td",
    "decimal-distinct.td",
    "decimal-join.td",
    "decimal-order.td",
    "decimal-overflow.td",
    "decimal-sum.td",
    "decimal-zero.td",
    "delete-using.td",
    "drop.td",
    "duplicate-table-names.td",
    "failpoints.td",
    "fetch-tail-large-diff.td",
    "fetch-tail-limit-timeout.td",
    "fetch-tail-timestamp-zero.td",
    "fetch-timeout.td",
    "float_sum.td",
    "get-started.td",
    "github-11563.td",
    "github-1947.td",
    "github-3281.td",
    "github-5502.td",
    "github-5774.td",
    "github-5873.td",
    "github-5983.td",
    "github-5984.td",
    "github-6335.td",
    "github-6744.td",
    "github-6950.td",
    "github-7171.td",
    "github-7191.td",
    "github-795.td",
    "joins.td",
    "jsonb.td",
    "list.td",
    # Flaky on Azure: https://buildkite.com/materialize/nightly/builds/11906#019661aa-2f41-43e1-b08f-6195c66a7ab9
    # "load-generator-key-value.td",
    "logging.td",
    "map.td",
    "multijoins.td",
    "numeric-sum.td",
    "numeric.td",
    "oid.td",
    "orms.td",
    "pg-catalog.td",
    "runtime-errors.td",
    "search_path.td",
    "self-test.td",
    "string.td",
    "subquery-scalar-errors.td",
    "system-functions.td",
    "test-skip-if.td",
    "tpch.td",
    "type_char_quoted.td",
    "version.td",
    # Hangs on GCP in check-shard-tombstone
    # "webhook.td",
]


def add_arguments_temporary_test(parser: WorkflowArgumentParser) -> None:
    parser.add_argument(
        "--setup",
        default=True,
        action=argparse.BooleanOptionalAction,
        help="Run setup steps",
    )
    parser.add_argument(
        "--cleanup",
        default=True,
        action=argparse.BooleanOptionalAction,
        help="Destroy the region at the end of the workflow.",
    )
    parser.add_argument(
        "--test",
        default=True,
        action=argparse.BooleanOptionalAction,
        help="Run the actual test part",
    )
    parser.add_argument(
        "--run-testdrive-files",
        default=True,
        action=argparse.BooleanOptionalAction,
        help="Run testdrive files",
    )
    parser.add_argument(
        "--run-mz-debug",
        default=True,
        action=argparse.BooleanOptionalAction,
        help="Run mz-debug",
    )
    parser.add_argument(
        "--tag",
        type=str,
        help="Custom version tag to use",
    )
    parser.add_argument(
        "files",
        nargs="*",
        default=COMPATIBLE_TESTDRIVE_FILES,
        help="run against the specified files",
    )


def run_ignore_error(
    args: Sequence[Path | str],
    cwd: Path | None = None,
    stdin: None | int | IO[bytes] | bytes = None,
    env: dict[str, str] | None = None,
):
    try:
        spawn.runv(args, cwd=cwd, stdin=stdin, env=env)
    except subprocess.CalledProcessError:
        pass


def testdrive(no_reset: bool) -> Testdrive:
    return Testdrive(
        materialize_url="postgres://materialize@127.0.0.1:6875/materialize",
        materialize_url_internal="postgres://mz_system:materialize@127.0.0.1:6877/materialize",
        materialize_use_https=False,
        no_consistency_checks=True,
        set_persist_urls=False,
        network_mode="host",
        volume_workdir="../testdrive:/workdir",
        no_reset=no_reset,
        default_timeout="360s",
        # For full testdrive support we'll need:
        # kafka_url=...
        # schema_registry_url=...
        # aws_endpoint=...
    )


def get_tag(tag: str | None) -> str:
    return tag or f"v{ci_util.get_mz_version()}--pr.g{git.rev_parse('HEAD')}"


def build_mz_debug_async(env: dict[str, str] | None = None) -> threading.Thread:
    def run():
        spawn.capture(
            [
                "cargo",
                "build",
                "--bin",
                "mz-debug",
            ],
            cwd=MZ_ROOT,
            stderr=subprocess.STDOUT,
            env=env,
        )

    thread = threading.Thread(target=run)
    thread.start()
    return thread


def run_mz_debug(env: dict[str, str] | None = None) -> None:
    print("--- Running mz-debug")
    try:
        # mz-debug (and its compilation) is rather noisy, so ignore the output
        spawn.capture(
            [
                "cargo",
                "run",
                "--bin",
                "mz-debug",
                "--",
                "self-managed",
                "--k8s-namespace",
                "materialize-environment",
                "--k8s-namespace",
                "materialize",
            ],
            cwd=MZ_ROOT,
            stderr=subprocess.STDOUT,
            env=env,
        )
    except:
        pass


class State:
    materialize_environment: dict | None
    path: Path
    environmentd_port_forward_process: subprocess.Popen[bytes] | None
    balancerd_port_forward_process: subprocess.Popen[bytes] | None
    version: int

    def __init__(self, path: Path):
        self.materialize_environment = None
        self.path = path
        self.environmentd_port_forward_process = None
        self.balancerd_port_forward_process = None
        self.version = 0

    def kubectl_setup(
        self, tag: str, metadata_backend_url: str, persist_backend_url: str
    ) -> None:
        self.metadata_backend_url = metadata_backend_url
        self.persist_backend_url = persist_backend_url
        spawn.runv(["kubectl", "get", "nodes"])

        for i in range(60):
            try:
                spawn.runv(
                    ["kubectl", "get", "pods", "-n", "materialize"],
                    cwd=self.path,
                )
                print("Logging all pods in materialize:")
                pod_names = (
                    spawn.capture(
                        [
                            "kubectl",
                            "get",
                            "pods",
                            "-n",
                            "materialize",
                            "-o",
                            "name",
                        ],
                        cwd=self.path,
                    )
                    .strip()
                    .split("\n")
                )
                for pod_name in pod_names:
                    spawn.runv(
                        [
                            "kubectl",
                            "logs",
                            "-n",
                            "materialize",
                            pod_name,
                            "--all-containers=true",
                        ],
                        cwd=self.path,
                    )
                status = spawn.capture(
                    [
                        "kubectl",
                        "get",
                        "pods",
                        "-n",
                        "materialize",
                        "-o",
                        "jsonpath={.items[0].status.phase}",
                    ],
                    cwd=self.path,
                )
                if status == "Running":
                    break
            except subprocess.CalledProcessError:
                time.sleep(1)
        else:
            raise ValueError("Never completed")

        spawn.runv(["kubectl", "create", "namespace", "materialize-environment"])

        materialize_backend_secret = {
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {
                "name": "materialize-backend",
                "namespace": "materialize-environment",
            },
            "stringData": {
                "metadata_backend_url": self.metadata_backend_url,
                "persist_backend_url": self.persist_backend_url,
                "license_key": os.getenv("MZ_CI_LICENSE_KEY"),
            },
        }

        spawn.runv(
            ["kubectl", "apply", "-f", "-"],
            cwd=self.path,
            stdin=yaml.dump(materialize_backend_secret).encode(),
        )

        self.materialize_environment = {
            "apiVersion": "materialize.cloud/v1alpha1",
            "kind": "Materialize",
            "metadata": {
                "name": "12345678-1234-1234-1234-123456789012",
                "namespace": "materialize-environment",
            },
            "spec": {
                "environmentdImageRef": f"materialize/environmentd:{tag}",
                "environmentdResourceRequirements": {
                    "limits": {"memory": "4Gi"},
                    "requests": {"cpu": "2", "memory": "4Gi"},
                },
                "balancerdResourceRequirements": {
                    "limits": {"memory": "256Mi"},
                    "requests": {"cpu": "100m", "memory": "256Mi"},
                },
                "backendSecretName": "materialize-backend",
                "authenticatorKind": "None",
            },
        }

        spawn.runv(
            ["kubectl", "apply", "-f", "-"],
            cwd=self.path,
            stdin=yaml.dump(self.materialize_environment).encode(),
        )
        for i in range(60):
            try:
                spawn.runv(
                    [
                        "kubectl",
                        "get",
                        "materializes",
                        "-n",
                        "materialize-environment",
                    ],
                    cwd=self.path,
                )
                break
            except subprocess.CalledProcessError:
                time.sleep(1)
        else:
            raise ValueError("Never completed")
        for i in range(240):
            try:
                spawn.runv(
                    ["kubectl", "get", "pods", "-n", "materialize-environment"],
                    cwd=self.path,
                )
                status = spawn.capture(
                    [
                        "kubectl",
                        "get",
                        "pods",
                        "-l",
                        "app=environmentd",
                        "-n",
                        "materialize-environment",
                        "-o",
                        "jsonpath={.items[0].status.phase}",
                    ],
                    cwd=self.path,
                )
                if status == "Running":
                    break
            except subprocess.CalledProcessError:
                time.sleep(1)
        else:
            print("Getting all pods:")
            spawn.runv(
                [
                    "kubectl",
                    "get",
                    "pods",
                    "-n",
                    "materialize-environment",
                ],
                cwd=self.path,
            )
            print("Describing all pods in materialize-environment:")
            spawn.runv(
                [
                    "kubectl",
                    "describe",
                    "pods",
                    "-n",
                    "materialize-environment",
                ],
                cwd=self.path,
            )
            raise ValueError("Never completed")

        # Can take a while for balancerd to come up
        for i in range(300):
            try:
                status = spawn.capture(
                    [
                        "kubectl",
                        "get",
                        "pods",
                        "-l",
                        "app=balancerd",
                        "-n",
                        "materialize-environment",
                        "-o",
                        "jsonpath={.items[0].status.phase}",
                    ],
                    cwd=self.path,
                )
                if status == "Running":
                    break
            except subprocess.CalledProcessError:
                time.sleep(1)
        else:
            print("Getting all pods:")
            spawn.runv(
                [
                    "kubectl",
                    "get",
                    "pods",
                    "-n",
                    "materialize-environment",
                ],
                cwd=self.path,
            )
            print("Describing all pods in materialize-environment:")
            spawn.runv(
                [
                    "kubectl",
                    "describe",
                    "pods",
                    "-n",
                    "materialize-environment",
                ],
                cwd=self.path,
            )
            raise ValueError("Never completed")

    def cleanup(self) -> None:
        if self.environmentd_port_forward_process:
            os.killpg(
                os.getpgid(self.environmentd_port_forward_process.pid), signal.SIGTERM
            )
        if self.balancerd_port_forward_process:
            os.killpg(
                os.getpgid(self.balancerd_port_forward_process.pid), signal.SIGTERM
            )

    def destroy(self, env=None) -> None:
        print("--- Destroying")
        if self.materialize_environment:
            run_ignore_error(
                ["kubectl", "delete", "-f", "-"],
                cwd=self.path,
                stdin=yaml.dump(self.materialize_environment).encode(),
            )
        run_ignore_error(
            [
                "kubectl",
                "delete",
                "materialize.materialize.cloud/12345678-1234-1234-1234-123456789012",
                "-n" "materialize-environment",
            ]
        )
        run_ignore_error(["kubectl", "delete", "namespace", "materialize-environment"])
        run_ignore_error(["kubectl", "delete", "namespace", "materialize"])
        spawn.runv(["terraform", "destroy", "-auto-approve"], cwd=self.path, env=env)

    def test(
        self, c: Composition, tag: str, run_testdrive_files: bool, files: list[str]
    ) -> None:
        print("--- Running tests")
        self.connect(c)
        time.sleep(10)

        with psycopg.connect(
            "postgres://materialize@127.0.0.1:6875/materialize"
        ) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                results = cur.fetchall()
                assert results == [(1,)], results
                cur.execute("SELECT mz_version()")
                version = cur.fetchall()[0][0]
                assert version.startswith(tag.split("--")[0] + " ")
                with open(
                    MZ_ROOT / "misc" / "helm-charts" / "operator" / "Chart.yaml"
                ) as f:
                    content = yaml.load(f, Loader=yaml.Loader)
                    helm_chart_version = content["version"]
                assert version.endswith(
                    f", helm chart: {helm_chart_version})"
                ), f"Actual version: {version}, expected to contain {helm_chart_version}"

        if run_testdrive_files:
            with c.override(testdrive(no_reset=False)):
                c.up("testdrive", persistent=True)
                c.run_testdrive_files(*TD_CMD, *files)

    def connect(self, c: Composition) -> None:
        environmentd_name = spawn.capture(
            [
                "kubectl",
                "get",
                "pods",
                "-l",
                "app=environmentd",
                "-n",
                "materialize-environment",
                "-o",
                "jsonpath={.items[*].metadata.name}",
            ],
            cwd=self.path,
        )

        balancerd_name = spawn.capture(
            [
                "kubectl",
                "get",
                "pods",
                "-l",
                "app=balancerd",
                "-n",
                "materialize-environment",
                "-o",
                "jsonpath={.items[*].metadata.name}",
            ],
            cwd=self.path,
        )
        # error: arguments in resource/name form must have a single resource and name
        print(f"Got balancerd name: {balancerd_name}")

        self.environmentd_port_forward_process = subprocess.Popen(
            [
                "kubectl",
                "port-forward",
                f"pod/{environmentd_name}",
                "-n",
                "materialize-environment",
                "6877:6877",
                "6878:6878",
            ],
            preexec_fn=os.setpgrp,
        )
        self.balancerd_port_forward_process = subprocess.Popen(
            [
                "kubectl",
                "port-forward",
                f"pod/{balancerd_name}",
                "-n",
                "materialize-environment",
                "6875:6875",
                "6876:6876",
            ],
            preexec_fn=os.setpgrp,
        )
        time.sleep(10)

        with psycopg.connect(
            "postgres://mz_system:materialize@127.0.0.1:6877/materialize",
            autocommit=True,
        ) as conn:
            with conn.cursor() as cur:
                # Required for some testdrive tests
                cur.execute("ALTER CLUSTER mz_system SET (REPLICATION FACTOR 2)")
                cur.execute("ALTER SYSTEM SET enable_create_table_from_source = true")

        with c.override(testdrive(no_reset=False)):
            c.up("testdrive", persistent=True)
            c.testdrive(
                dedent(
                    """
               > SELECT 1
               1
            """
                )
            )


class AWS(State):
    def setup(
        self,
        prefix: str,
        setup: bool,
        tag: str,
        orchestratord_tag: str | None = None,
    ) -> None:
        if not setup:
            spawn.runv(
                [
                    "aws",
                    "eks",
                    "update-kubeconfig",
                    "--name",
                    f"{prefix}-dev-eks",
                    "--region",
                    "us-east-1",
                ]
            )
            return

        vars = [
            "-var",
            "operator_version=v25.3.0-beta.1",
        ]
        vars += [
            "-var",
            f"orchestratord_version={get_tag(orchestratord_tag or tag)}",
        ]

        print("--- Setup")
        spawn.runv(
            ["helm", "package", "../../../misc/helm-charts/operator/"], cwd=self.path
        )
        spawn.runv(["terraform", "init"], cwd=self.path)
        spawn.runv(["terraform", "validate"], cwd=self.path)
        spawn.runv(["terraform", "plan", *vars], cwd=self.path)
        try:
            spawn.runv(["terraform", "apply", "-auto-approve", *vars], cwd=self.path)
        except:
            # Sometimes fails for unknown reason, so just retry:
            # > Error: namespaces is forbidden: User "arn:aws:sts::400121260767:assumed-role/ci/ci" cannot create resource "namespaces" in API group "" at the cluster scope
            spawn.runv(["terraform", "apply", "-auto-approve", *vars], cwd=self.path)

        spawn.runv(
            [
                "aws",
                "eks",
                "update-kubeconfig",
                "--name",
                f"{prefix}-dev-eks",
                "--region",
                "us-east-1",
            ]
        )

        metadata_backend_url = spawn.capture(
            ["terraform", "output", "-raw", "metadata_backend_url"], cwd=self.path
        ).strip()
        persist_backend_url = spawn.capture(
            ["terraform", "output", "-raw", "persist_backend_url"], cwd=self.path
        ).strip()
        self.kubectl_setup(tag, metadata_backend_url, persist_backend_url)

    def upgrade(self, tag: str) -> None:
        print(f"--- Upgrading to {tag}")
        # Following https://materialize.com/docs/self-managed/v25.1/installation/install-on-aws/upgrade-on-aws/
        self.materialize_environment = {
            "apiVersion": "materialize.cloud/v1alpha1",
            "kind": "Materialize",
            "metadata": {
                "name": "12345678-1234-1234-1234-123456789012",
                "namespace": "materialize-environment",
            },
            "spec": {
                "inPlaceRollout": True,
                "requestRollout": f"12345678-9012-3456-7890-12345678901{self.version+3}",
                "environmentdImageRef": f"materialize/environmentd:{tag}",
                "environmentdResourceRequirements": {
                    "limits": {"memory": "4Gi"},
                    "requests": {"cpu": "2", "memory": "4Gi"},
                },
                "balancerdResourceRequirements": {
                    "limits": {"memory": "256Mi"},
                    "requests": {"cpu": "100m", "memory": "256Mi"},
                },
                "backendSecretName": "materialize-backend",
                "authenticatorKind": "None",
            },
        }

        self.version += 1
        spawn.runv(
            ["kubectl", "apply", "-f", "-"],
            cwd=self.path,
            stdin=yaml.dump(self.materialize_environment).encode(),
        )
        for i in range(60):
            try:
                spawn.runv(
                    [
                        "kubectl",
                        "get",
                        "materializes",
                        "-n",
                        "materialize-environment",
                    ],
                    cwd=self.path,
                )
                break
            except subprocess.CalledProcessError:
                time.sleep(1)
        else:
            raise ValueError("Never completed")
        for i in range(240):
            try:
                spawn.runv(
                    ["kubectl", "get", "pods", "-n", "materialize-environment"],
                    cwd=self.path,
                )
                status = spawn.capture(
                    [
                        "kubectl",
                        "get",
                        "pods",
                        "-l",
                        "app=environmentd",
                        "-n",
                        "materialize-environment",
                        "-o",
                        "jsonpath={.items[0].status.phase}",
                    ],
                    cwd=self.path,
                )
                if status == "Running":
                    break
            except subprocess.CalledProcessError:
                time.sleep(1)
        else:
            raise ValueError("Never completed")

        # Can take a while for balancerd to come up
        for i in range(300):
            try:
                status = spawn.capture(
                    [
                        "kubectl",
                        "get",
                        "pods",
                        "-l",
                        "app=balancerd",
                        "-n",
                        "materialize-environment",
                        "-o",
                        "jsonpath={.items[0].status.phase}",
                    ],
                    cwd=self.path,
                )
                if status == "Running":
                    break
            except subprocess.CalledProcessError:
                time.sleep(1)
        else:
            raise ValueError("Never completed")


def workflow_aws_temporary(c: Composition, parser: WorkflowArgumentParser) -> None:
    """To run locally use `aws sso login` first."""
    add_arguments_temporary_test(parser)
    args = parser.parse_args()

    tag = get_tag(args.tag)
    path = MZ_ROOT / "test" / "terraform" / "aws-temporary"
    aws = AWS(path)
    mz_debug_build_thread: threading.Thread | None = None
    try:
        if args.run_mz_debug:
            mz_debug_build_thread = build_mz_debug_async()
        aws.setup("aws-test", args.setup, tag)
        if args.test:
            aws.test(c, tag, args.run_testdrive_files, args.files)
    finally:
        aws.cleanup()

        if args.run_mz_debug:
            assert mz_debug_build_thread
            mz_debug_build_thread.join()
            run_mz_debug()

        if args.cleanup:
            aws.destroy()


def workflow_aws_upgrade(c: Composition, parser: WorkflowArgumentParser) -> None:
    """To run locally use `aws sso login` first."""
    add_arguments_temporary_test(parser)
    args = parser.parse_args()

    previous_tags = get_self_managed_versions()
    tag = get_tag(args.tag)
    path = MZ_ROOT / "test" / "terraform" / "aws-upgrade"
    aws = AWS(path)
    mz_debug_build_thread: threading.Thread | None = None
    try:
        if args.run_mz_debug:
            mz_debug_build_thread = build_mz_debug_async()
        aws.setup("aws-upgrade", args.setup, str(previous_tags[0]), str(tag))
        for previous_tag in previous_tags[1:]:
            aws.upgrade(str(previous_tag))
        aws.upgrade(tag)
        if args.test:
            # Try waiting a bit, otherwise connection error, should be handled better
            time.sleep(180)
            print("--- Running tests")
            aws.connect(c)

            with psycopg.connect(
                "postgres://materialize@127.0.0.1:6875/materialize"
            ) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
                    results = cur.fetchall()
                    assert results == [(1,)], results
                    cur.execute("SELECT mz_version()")
                    version = cur.fetchall()[0][0]
                    assert version.startswith(
                        tag.split("--")[0] + " "
                    ), f"Version expected to start with {tag.split('--')[0]}, but is actually {version}"
                    with open(
                        MZ_ROOT / "misc" / "helm-charts" / "operator" / "Chart.yaml"
                    ) as f:
                        content = yaml.load(f, Loader=yaml.Loader)
                        helm_chart_version = content["version"]
                    assert version.endswith(
                        f", helm chart: {helm_chart_version})"
                    ), f"Actual version: {version}, expected to contain {helm_chart_version}"

            if args.run_testdrive_files:
                with c.override(testdrive(no_reset=False)):
                    c.up("testdrive", persistent=True)
                    c.run_testdrive_files(*TD_CMD, *args.files)
    finally:
        aws.cleanup()

        if args.run_mz_debug:
            assert mz_debug_build_thread
            mz_debug_build_thread.join()
            run_mz_debug()

        if args.cleanup:
            aws.destroy()


PATH_AWS_PERSISTENT = MZ_ROOT / "test" / "terraform" / "aws-persistent"
PREFIX_AWS_PERSISTENT = "aws-persistent"


def workflow_aws_persistent_setup(
    c: Composition, parser: WorkflowArgumentParser
) -> None:
    """Setup the AWS persistent Terraform and Helm Chart"""
    parser.add_argument(
        "--tag",
        type=str,
        help="Custom version tag to use",
    )

    args = parser.parse_args()

    tag = get_tag(args.tag)
    aws = AWS(PATH_AWS_PERSISTENT)
    try:
        aws.setup(PREFIX_AWS_PERSISTENT, True, tag)
        with c.override(testdrive(no_reset=True)):
            aws.connect(c)
            c.testdrive(
                dedent(
                    """
               > CREATE SOURCE counter FROM LOAD GENERATOR COUNTER
               > CREATE TABLE table (c INT)
               > CREATE MATERIALIZED VIEW mv AS SELECT count(*) FROM table
            """
                )
            )
    finally:
        aws.cleanup()


def workflow_aws_persistent_test(
    c: Composition, parser: WorkflowArgumentParser
) -> None:
    """Run a test workload against the AWS persistent setup"""
    parser.add_argument(
        "--tag",
        type=str,
        help="Custom version tag to use",
    )

    parser.add_argument("--runtime", default=600, type=int, help="Runtime in seconds")

    args = parser.parse_args()

    start_time = time.time()

    tag = get_tag(args.tag)
    aws = AWS(PATH_AWS_PERSISTENT)
    try:
        aws.setup(PREFIX_AWS_PERSISTENT, False, tag)
        with c.override(testdrive(no_reset=True)):
            aws.connect(c)

            count = 1

            c.testdrive(
                dedent(
                    """
               > DELETE FROM table
                """
                )
            )

            while time.time() - start_time < args.runtime:
                c.testdrive(
                    dedent(
                        f"""
                   > SELECT 1
                   1

                   > INSERT INTO table VALUES ({count})

                   > SELECT count(*) FROM table
                   {count}

                   > SELECT * FROM mv
                   {count}

                   > DROP VIEW IF EXISTS temp

                   > CREATE VIEW temp AS SELECT * FROM mv

                   > SELECT * FROM temp
                   {count}
                """
                    )
                )

                count += 1

                with psycopg.connect(
                    "postgres://materialize@127.0.0.1:6875/materialize", autocommit=True
                ) as conn:
                    with conn.cursor() as cur:
                        cur.execute("SELECT max(counter) FROM counter")
                        old_max = cur.fetchall()[0][0]
                    time.sleep(5)
                    with conn.cursor() as cur:
                        cur.execute("SELECT max(counter) FROM counter")
                        new_max = cur.fetchall()[0][0]
                assert new_max > old_max, f"{new_max} should be greater than {old_max}"
    finally:
        aws.cleanup()


def workflow_aws_persistent_destroy(
    c: Composition, parser: WorkflowArgumentParser
) -> None:
    """Setup the AWS persistent Terraform and Helm Chart"""
    aws = AWS(PATH_AWS_PERSISTENT)
    aws.destroy()


def workflow_gcp_temporary(c: Composition, parser: WorkflowArgumentParser) -> None:
    add_arguments_temporary_test(parser)
    args = parser.parse_args()

    tag = get_tag(args.tag)
    path = MZ_ROOT / "test" / "terraform" / "gcp-temporary"
    state = State(path)

    gcp_service_account_json = os.getenv("GCP_SERVICE_ACCOUNT_JSON")
    assert (
        gcp_service_account_json
    ), "GCP_SERVICE_ACCOUNT_JSON environment variable has to be set"
    gcloud_creds_path = path / "gcp.json"
    with open(gcloud_creds_path, "w") as f:
        f.write(gcp_service_account_json)
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(gcloud_creds_path)

    mz_debug_build_thread: threading.Thread | None = None
    try:
        if args.run_mz_debug:
            mz_debug_build_thread = build_mz_debug_async()
        spawn.runv(["gcloud", "config", "set", "project", "materialize-ci"])

        spawn.runv(
            [
                "gcloud",
                "auth",
                "activate-service-account",
                f"--key-file={gcloud_creds_path}",
            ],
        )

        vars = [
            "-var",
            "operator_version=v25.3.0-beta.1",
        ]
        vars += [
            "-var",
            f"orchestratord_version={get_tag(tag)}",
        ]

        if args.setup:
            print("--- Setup")
            spawn.runv(
                ["helm", "package", "../../../misc/helm-charts/operator/"],
                cwd=path,
            )
            spawn.runv(["terraform", "init"], cwd=path)
            spawn.runv(["terraform", "validate"], cwd=path)
            spawn.runv(["terraform", "plan"], cwd=path)
            spawn.runv(["terraform", "apply", "-auto-approve", *vars], cwd=path)

        gke_cluster = json.loads(
            spawn.capture(
                ["terraform", "output", "-json", "gke_cluster"], cwd=path
            ).strip()
        )
        connection_strings = json.loads(
            spawn.capture(
                ["terraform", "output", "-json", "connection_strings"], cwd=path
            ).strip()
        )

        spawn.runv(
            [
                "gcloud",
                "container",
                "clusters",
                "get-credentials",
                gke_cluster["name"],
                "--region",
                gke_cluster["location"],
                "--project",
                "materialize-ci",
            ]
        )

        if args.setup:
            print("--- Setup")
            state.kubectl_setup(
                tag,
                connection_strings["metadata_backend_url"],
                connection_strings["persist_backend_url"],
            )

        if args.test:
            state.test(c, tag, args.run_testdrive_files, args.files)
    finally:
        state.cleanup()

        if args.run_mz_debug:
            assert mz_debug_build_thread
            mz_debug_build_thread.join()
            run_mz_debug()

        if args.cleanup:
            state.destroy()


def workflow_azure_temporary(c: Composition, parser: WorkflowArgumentParser) -> None:
    add_arguments_temporary_test(parser)
    args = parser.parse_args()

    tag = get_tag(args.tag)
    path = MZ_ROOT / "test" / "terraform" / "azure-temporary"
    state = State(path)

    spawn.runv(["bin/ci-builder", "run", "stable", "uv", "venv", str(path / "venv")])
    venv_env = os.environ.copy()
    venv_env["PATH"] = f"{path/'venv'/'bin'}:{os.getenv('PATH')}"
    venv_env["VIRTUAL_ENV"] = str(path / "venv")
    spawn.runv(
        ["uv", "pip", "install", "-r", "requirements.txt", "--prerelease=allow"],
        cwd=path,
        env=venv_env,
    )

    mz_debug_build_thread: threading.Thread | None = None
    try:
        if args.run_mz_debug:
            mz_debug_build_thread = build_mz_debug_async()
        if os.getenv("CI"):
            username = os.getenv("AZURE_SERVICE_ACCOUNT_USERNAME")
            password = os.getenv("AZURE_SERVICE_ACCOUNT_PASSWORD")
            tenant = os.getenv("AZURE_SERVICE_ACCOUNT_TENANT")
            assert username, "AZURE_SERVICE_ACCOUNT_USERNAME has to be set"
            assert password, "AZURE_SERVICE_ACCOUNT_PASSWORD has to be set"
            assert tenant, "AZURE_SERVICE_ACOUNT_TENANT has to be set"
            subprocess.run(
                [
                    "az",
                    "login",
                    "--service-principal",
                    "--username",
                    username,
                    "--password",
                    password,
                    "--tenant",
                    tenant,
                ],
                env=venv_env,
            )

        vars = [
            "-var",
            "operator_version=v25.3.0-beta.1",
        ]
        vars += [
            "-var",
            f"orchestratord_version={get_tag(tag)}",
        ]

        if args.setup:
            spawn.runv(
                ["helm", "package", "../../../misc/helm-charts/operator/"],
                cwd=path,
            )
            spawn.runv(["terraform", "init"], cwd=path, env=venv_env)
            spawn.runv(["terraform", "validate"], cwd=path, env=venv_env)
            spawn.runv(["terraform", "plan"], cwd=path, env=venv_env)
            try:
                spawn.runv(
                    ["terraform", "apply", "-auto-approve", *vars],
                    cwd=path,
                    env=venv_env,
                )
            except:
                print("terraform apply failed, retrying")
                spawn.runv(
                    ["terraform", "apply", "-auto-approve", *vars],
                    cwd=path,
                    env=venv_env,
                )

        aks_cluster = json.loads(
            spawn.capture(
                ["terraform", "output", "-json", "aks_cluster"], cwd=path, env=venv_env
            ).strip()
        )
        connection_strings = json.loads(
            spawn.capture(
                ["terraform", "output", "-json", "connection_strings"],
                cwd=path,
                env=venv_env,
            ).strip()
        )
        resource_group_name = spawn.capture(
            ["terraform", "output", "-raw", "resource_group_name"],
            cwd=path,
            env=venv_env,
        ).strip()

        spawn.runv(
            [
                "az",
                "aks",
                "get-credentials",
                "--overwrite-existing",
                "--resource-group",
                resource_group_name,
                "--name",
                aks_cluster["name"],
            ],
            env=venv_env,
        )

        if args.setup:
            state.kubectl_setup(
                tag,
                connection_strings["metadata_backend_url"],
                connection_strings["persist_backend_url"],
            )

        if args.test:
            state.test(c, tag, args.run_testdrive_files, args.files)
    finally:
        state.cleanup()

        if args.run_mz_debug:
            assert mz_debug_build_thread
            mz_debug_build_thread.join()
            run_mz_debug(env=venv_env)

        if args.cleanup:
            state.destroy(env=venv_env)
