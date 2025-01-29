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

SERVICES = [
    Testdrive(),  # Overridden below
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
    "fetch-tail-as-of.td",
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
    "type_char_quoted.td",
    "version.td",
]


def run_ignore_error(
    args: Sequence[Path | str],
    cwd: Path | None = None,
    stdin: None | int | IO[bytes] | bytes = None,
):
    try:
        spawn.runv(args, cwd=cwd, stdin=stdin)
    except subprocess.CalledProcessError:
        pass


def workflow_aws(c: Composition, parser: WorkflowArgumentParser) -> None:
    """To run locally use `aws sso login` first."""
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

    args = parser.parse_args()

    tag = args.tag or f"v{ci_util.get_mz_version()}--pr.g{git.rev_parse('HEAD')}"
    materialize_environment = None
    environmentd_port_forward_process = None
    balancerd_port_forward_process = None

    path = MZ_ROOT / "test" / "terraform" / "aws"
    try:
        if args.setup:
            print("--- Setup")
            spawn.runv(["terraform", "init"], cwd=path)
            spawn.runv(["terraform", "validate"], cwd=path)
            spawn.runv(["terraform", "plan"], cwd=path)
            spawn.runv(["terraform", "apply", "-auto-approve"], cwd=path)

            metadata_backend_url = spawn.capture(
                ["terraform", "output", "-raw", "metadata_backend_url"], cwd=path
            ).strip()
            persist_backend_url = spawn.capture(
                ["terraform", "output", "-raw", "persist_backend_url"], cwd=path
            ).strip()
            materialize_s3_role_arn = spawn.capture(
                ["terraform", "output", "-raw", "materialize_s3_role_arn"], cwd=path
            ).strip()

            spawn.runv(
                [
                    "aws",
                    "eks",
                    "update-kubeconfig",
                    "--name",
                    "terraform-aws-test-dev-eks",
                    "--region",
                    "us-east-1",
                ]
            )

            spawn.runv(["kubectl", "get", "nodes"])
            # Not working yet?
            # spawn.runv(
            #     ["helm", "repo", "add", "openebs", "https://openebs.github.io/openebs"]
            # )
            # spawn.runv(["helm", "repo", "update"])
            # spawn.runv(
            #     [
            #         "helm",
            #         "install",
            #         "openebs",
            #         "--namespace",
            #         "openebs",
            #         "openebs/openebs",
            #         "--set",
            #         "engines.replicated.mayastor.enabled=false",
            #         "--create-namespace",
            #     ]
            # )
            # spawn.runv(
            #     ["kubectl", "get", "pods", "-n", "openebs", "-l", "role=openebs-lvm"]
            # )

            aws_account_id = spawn.capture(
                [
                    "aws",
                    "sts",
                    "get-caller-identity",
                    "--query",
                    "Account",
                    "--output",
                    "text",
                ]
            ).strip()
            public_ip_address = spawn.capture(
                ["curl", "http://checkip.amazonaws.com"]
            ).strip()

            materialize_values = {
                "operator": {
                    "image": {"tag": tag},
                    "cloudProvider": {
                        "type": "aws",
                        "region": "us-east-1",
                        "providers": {
                            "aws": {
                                "enabled": True,
                                "accountID": aws_account_id,
                                "iam": {
                                    "roles": {"environment": materialize_s3_role_arn}
                                },
                            }
                        },
                    },
                },
                "rbac": {"enabled": False},
                "networkPolicies": {
                    "enabled": True,
                    "egress": {"enabled": True, "cidrs": ["0.0.0.0/0"]},
                    "ingress": {"enabled": True, "cidrs": [f"{public_ip_address}/24"]},
                    "internal": {"enabled": True},
                },
            }

            spawn.runv(
                [
                    "helm",
                    "install",
                    "materialize-operator",
                    "misc/helm-charts/operator",
                    "--namespace",
                    "materialize",
                    "--create-namespace",
                    "-f",
                    "-",
                ],
                cwd=MZ_ROOT,
                stdin=yaml.dump(materialize_values).encode(),
            )
            for i in range(60):
                try:
                    spawn.runv(
                        ["kubectl", "get", "pods", "-n", "materialize"],
                        cwd=path,
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
                        cwd=path,
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
                    "metadata_backend_url": metadata_backend_url,
                    "persist_backend_url": persist_backend_url,
                },
            }

            spawn.runv(
                ["kubectl", "apply", "-f", "-"],
                cwd=path,
                stdin=yaml.dump(materialize_backend_secret).encode(),
            )

            materialize_environment = {
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
                },
            }

            spawn.runv(
                ["kubectl", "apply", "-f", "-"],
                cwd=path,
                stdin=yaml.dump(materialize_environment).encode(),
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
                        cwd=path,
                    )
                    break
                except subprocess.CalledProcessError:
                    time.sleep(1)
            else:
                raise ValueError("Never completed")
            for i in range(180):
                try:
                    spawn.runv(
                        ["kubectl", "get", "pods", "-n", "materialize-environment"],
                        cwd=path,
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
                        cwd=path,
                    )
                    if status == "Running":
                        break
                except subprocess.CalledProcessError:
                    time.sleep(1)
            else:
                raise ValueError("Never completed")

            # Can take a while for balancerd to come up
            for i in range(240):
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
                        cwd=path,
                    )
                    if status == "Running":
                        break
                except subprocess.CalledProcessError:
                    time.sleep(1)
            else:
                raise ValueError("Never completed")
        else:
            spawn.runv(
                [
                    "aws",
                    "eks",
                    "update-kubeconfig",
                    "--name",
                    "terraform-aws-test-dev-eks",
                    "--region",
                    "us-east-1",
                ]
            )

        print("--- Running tests")
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
            cwd=path,
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
            cwd=path,
        )
        # error: arguments in resource/name form must have a single resource and name
        print(f"Got balancerd name: {balancerd_name}")

        environmentd_port_forward_process = subprocess.Popen(
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
        balancerd_port_forward_process = subprocess.Popen(
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

        with c.override(
            Testdrive(
                materialize_url="postgres://materialize@127.0.0.1:6875/materialize",
                materialize_url_internal="postgres://mz_system:materialize@127.0.0.1:6877/materialize",
                materialize_use_https=True,
                no_consistency_checks=True,
                network_mode="host",
                volume_workdir="../testdrive:/workdir",
                # For full testdrive support we'll need:
                # kafka_url=...
                # schema_registry_url=...
                # aws_endpoint=...
            )
        ):
            c.up("testdrive", persistent=True)
            c.testdrive(
                dedent(
                    """
               > SELECT 1
               1
            """
                )
            )

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
                    assert version.endswith(f", helm chart: {helm_chart_version})")

            c.run_testdrive_files(*args.files)
    finally:
        if environmentd_port_forward_process:
            os.killpg(os.getpgid(environmentd_port_forward_process.pid), signal.SIGTERM)
        if balancerd_port_forward_process:
            os.killpg(os.getpgid(balancerd_port_forward_process.pid), signal.SIGTERM)

        if args.cleanup:
            print("--- Cleaning up")
            if materialize_environment:
                run_ignore_error(
                    ["kubectl", "delete", "-f", "-"],
                    cwd=path,
                    stdin=yaml.dump(materialize_environment).encode(),
                )
            run_ignore_error(
                [
                    "kubectl",
                    "delete",
                    "materialize.materialize.cloud/12345678-1234-1234-1234-123456789012",
                    "-n" "materialize-environment",
                ]
            )
            run_ignore_error(
                ["kubectl", "delete", "namespace", "materialize-environment"]
            )
            run_ignore_error(
                ["helm", "uninstall", "materialize-operator"],
                cwd=path,
            )
            run_ignore_error(["kubectl", "delete", "namespace", "materialize"])
            spawn.runv(["terraform", "destroy", "-auto-approve"], cwd=path)


def workflow_gcp(c: Composition, parser: WorkflowArgumentParser) -> None:
    """To run locally use `aws sso login` first."""
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

    args = parser.parse_args()

    tag = args.tag or f"v{ci_util.get_mz_version()}--pr.g{git.rev_parse('HEAD')}"
    materialize_environment = None
    environmentd_port_forward_process = None
    balancerd_port_forward_process = None

    path = MZ_ROOT / "test" / "terraform" / "gcp"

    gcp_service_account_json = os.getenv("GCP_SERVICE_ACCOUNT_JSON")
    assert (
        gcp_service_account_json
    ), "GCP_SERVICE_ACCOUNT_JSON environment variable has to be set"
    gcloud_creds_path = path / "gcp.json"
    with open(gcloud_creds_path, "w") as f:
        f.write(gcp_service_account_json)
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(gcloud_creds_path)

    try:
        spawn.runv(["gcloud", "config", "set", "project", "materialize-ci"])

        spawn.runv(
            [
                "gcloud",
                "auth",
                "activate-service-account",
                f"--key-file={gcloud_creds_path}",
            ],
        )

        if args.setup:
            print("--- Setup")
            spawn.runv(["terraform", "init"], cwd=path)
            spawn.runv(["terraform", "validate"], cwd=path)
            spawn.runv(["terraform", "plan"], cwd=path)
            spawn.runv(["terraform", "apply", "-auto-approve"], cwd=path)

        json.loads(
            spawn.capture(
                ["terraform", "output", "-json", "service_accounts"], cwd=path
            ).strip()
        )
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

        spawn.runv(["kubectl", "get", "nodes"])

        if args.setup:
            public_ip_address = spawn.capture(
                ["curl", "http://checkip.amazonaws.com"]
            ).strip()

            materialize_values = {
                "operator": {
                    "image": {"tag": tag},
                    "cloudProvider": {
                        "type": "gcp",
                        "region": "us-east1",
                        "providers": {
                            "gcp": {
                                "enabled": True,
                            }
                        },
                    },
                },
                "rbac": {"enabled": False},
                "networkPolicies": {
                    "enabled": True,
                    "egress": {"enabled": True, "cidrs": ["0.0.0.0/0"]},
                    "ingress": {"enabled": True, "cidrs": [f"{public_ip_address}/24"]},
                    "internal": {"enabled": True},
                },
            }

            spawn.runv(
                [
                    "helm",
                    "install",
                    "materialize-operator",
                    "misc/helm-charts/operator",
                    "--namespace",
                    "materialize",
                    "--create-namespace",
                    "-f",
                    "-",
                ],
                cwd=MZ_ROOT,
                stdin=yaml.dump(materialize_values).encode(),
            )
            for i in range(60):
                try:
                    spawn.runv(
                        ["kubectl", "get", "pods", "-n", "materialize"],
                        cwd=path,
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
                        cwd=path,
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
                    "metadata_backend_url": connection_strings["metadata_backend_url"],
                    "persist_backend_url": connection_strings["persist_backend_url"],
                },
            }

            spawn.runv(
                ["kubectl", "apply", "-f", "-"],
                cwd=path,
                stdin=yaml.dump(materialize_backend_secret).encode(),
            )

            materialize_environment = {
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
                },
            }

            spawn.runv(
                ["kubectl", "apply", "-f", "-"],
                cwd=path,
                stdin=yaml.dump(materialize_environment).encode(),
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
                        cwd=path,
                    )
                    break
                except subprocess.CalledProcessError:
                    time.sleep(1)
            else:
                raise ValueError("Never completed")
            for i in range(180):
                try:
                    spawn.runv(
                        ["kubectl", "get", "pods", "-n", "materialize-environment"],
                        cwd=path,
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
                        cwd=path,
                    )
                    if status == "Running":
                        break
                except subprocess.CalledProcessError:
                    time.sleep(1)
            else:
                raise ValueError("Never completed")

            # Can take a while for balancerd to come up
            for i in range(240):
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
                        cwd=path,
                    )
                    if status == "Running":
                        break
                except subprocess.CalledProcessError:
                    time.sleep(1)
            else:
                raise ValueError("Never completed")

        print("--- Running tests")
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
            cwd=path,
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
            cwd=path,
        )
        # error: arguments in resource/name form must have a single resource and name
        print(f"Got balancerd name: {balancerd_name}")

        environmentd_port_forward_process = subprocess.Popen(
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
        balancerd_port_forward_process = subprocess.Popen(
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

        with c.override(
            Testdrive(
                materialize_url="postgres://materialize@127.0.0.1:6875/materialize",
                materialize_url_internal="postgres://mz_system:materialize@127.0.0.1:6877/materialize",
                materialize_use_https=True,
                no_consistency_checks=True,
                network_mode="host",
                volume_workdir="../testdrive:/workdir",
                # For full testdrive support we'll need:
                # kafka_url=...
                # schema_registry_url=...
                # aws_endpoint=...
            )
        ):
            c.up("testdrive", persistent=True)
            c.testdrive(
                dedent(
                    """
               > SELECT 1
               1
            """
                )
            )

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
                    assert version.endswith(f", helm chart: {helm_chart_version})")

            c.run_testdrive_files(*args.files)
    finally:
        if environmentd_port_forward_process:
            os.killpg(os.getpgid(environmentd_port_forward_process.pid), signal.SIGTERM)
        if balancerd_port_forward_process:
            os.killpg(os.getpgid(balancerd_port_forward_process.pid), signal.SIGTERM)

        if args.cleanup:
            print("--- Cleaning up")
            if materialize_environment:
                run_ignore_error(
                    ["kubectl", "delete", "-f", "-"],
                    cwd=path,
                    stdin=yaml.dump(materialize_environment).encode(),
                )
            run_ignore_error(
                [
                    "kubectl",
                    "delete",
                    "materialize.materialize.cloud/12345678-1234-1234-1234-123456789012",
                    "-n" "materialize-environment",
                ]
            )
            run_ignore_error(
                ["kubectl", "delete", "namespace", "materialize-environment"]
            )
            run_ignore_error(
                ["helm", "uninstall", "materialize-operator"],
                cwd=path,
            )
            run_ignore_error(["kubectl", "delete", "namespace", "materialize"])
            spawn.runv(["terraform", "destroy", "-auto-approve"], cwd=path)
