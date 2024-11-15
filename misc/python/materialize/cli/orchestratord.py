# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
#
# orchestratord.py — build and run environments in a local kind cluster

import argparse
import os
import shutil
import subprocess
from textwrap import dedent
from time import sleep
from urllib.parse import urlparse, urlunparse

from materialize import MZ_ROOT, ui

DEV_IMAGE_TAG = "local-dev"
CONSOLE_VERSION = "25.1.0-beta.1"
DEFAULT_POSTGRES = (
    "postgres://root@postgres.materialize.svc.cluster.local:5432/materialize"
)
DEFAULT_MINIO = "s3://minio:minio123@persist/persist?endpoint=http%3A%2F%2Fminio.materialize.svc.cluster.local%3A9000&region=minio"


def main():
    os.chdir(MZ_ROOT)

    parser = argparse.ArgumentParser(
        prog="orchestratord",
        description="""Runs orchestratord within a local kind cluster""",
    )
    parser.add_argument(
        "--kind-cluster-name",
        default=os.environ.get("KIND_CLUSTER_NAME", "kind"),
    )
    subparsers = parser.add_subparsers(dest="subcommand")

    parser_run = subparsers.add_parser("run")
    parser_run.add_argument("--dev", action="store_true")
    parser_run.add_argument("--namespace", default="materialize")
    parser_run.set_defaults(func=run)

    parser_reset = subparsers.add_parser("reset")
    parser_reset.set_defaults(func=reset)

    parser_environment = subparsers.add_parser("environment")
    parser_environment.add_argument("--dev", action="store_true")
    parser_environment.add_argument("--namespace", default="materialize")
    parser_environment.add_argument(
        "--environment-id",
        default="12345678-1234-1234-1234-123456789012",
    )
    parser_environment.add_argument("--postgres-url", default=DEFAULT_POSTGRES)
    parser_environment.add_argument("--s3-bucket", default=DEFAULT_MINIO)
    parser_environment.set_defaults(func=environment)

    args = parser.parse_args()
    if args.subcommand is None:
        run(args)
    else:
        args.func(args)


def run(args: argparse.Namespace):
    acquire(
        "orchestratord",
        dev=args.dev,
        cluster=args.kind_cluster_name,
    )
    subprocess.check_call(
        [
            "helm",
            "install",
            "orchestratord",
            "misc/helm-charts/operator",
            "--atomic",
            f"--set=operator.image.tag={DEV_IMAGE_TAG}",
            f'--set-json=operator.args.consoleImageTagMapOverride={{"{DEV_IMAGE_TAG}":"{CONSOLE_VERSION}"}}',
            "--set=namespace.create=true",
            f"--set=namespace.name={args.namespace}",
        ]
    )


def reset(args: argparse.Namespace):
    environments = (
        kubectl(
            "get",
            "--all-namespaces",
            "materializes",
            '-o=jsonpath={range .items[*]}{.metadata.namespace}/{.metadata.name}{"\\n"}{end}',
            cluster=args.kind_cluster_name,
        )
        .decode()
        .splitlines()
    )
    for environment in environments:
        (namespace, environment_id) = environment.split("/", 1)

        def env_kubectl(*cmd_args: str, **subprocess_args):
            return kubectl(
                "-n",
                namespace,
                *cmd_args,
                cluster=args.kind_cluster_name,
                **subprocess_args,
            )

        secret_name = env_kubectl(
            "get",
            "materialize",
            environment_id,
            "-o=jsonpath={.spec.backendSecretName}",
        )
        assert secret_name is not None

        env_kubectl("delete", "--wait=true", "materialize", environment_id)
        env_kubectl("delete", "--wait=true", "secret", secret_name)

    try:
        subprocess.check_call(["helm", "uninstall", "orchestratord"])
    except subprocess.CalledProcessError:
        pass


def environment(args: argparse.Namespace):
    for image in ["environmentd", "clusterd", "balancerd"]:
        acquire(
            image,
            dev=args.dev,
            cluster=args.kind_cluster_name,
        )
    environmentd_image_ref = f"materialize/environmentd:{DEV_IMAGE_TAG}"

    try:
        kubectl(
            "get",
            "namespace",
            args.namespace,
            cluster=args.kind_cluster_name,
        )
    except subprocess.CalledProcessError:
        kubectl(
            "create",
            "namespace",
            args.namespace,
            cluster=args.kind_cluster_name,
        )

    def env_kubectl(*cmd_args: str, **subprocess_args):
        return kubectl(
            "-n",
            args.namespace,
            *cmd_args,
            cluster=args.kind_cluster_name,
            **subprocess_args,
        )

    def root_psql(cmd: str):
        env_kubectl(
            "run",
            "psql-setup",
            "--labels=app=environmentd",
            "--image=postgres",
            "--command=true",
            "--restart=Never",
            "--attach=true",
            "--rm=true",
            "--",
            "psql",
            args.postgres_url,
            "-c",
            cmd,
        )
        env_kubectl("wait", "--for=delete", "pod/psql-setup")

    pg_user = f"materialize_{args.environment_id}"
    pg_pass = "password"
    pg_db = f"materialize_{args.environment_id}"

    try:
        root_psql(f"""create role "{pg_user}" with nologin""")
        root_psql(f"""alter role "{pg_user}" with login password '{pg_pass}'""")
        root_psql(f"""create database "{pg_db}" with owner "{pg_user}" """)
    except subprocess.CalledProcessError:
        pass

    postgres_url_parts = urlparse(args.postgres_url)
    metadata_backend_url = urlunparse(
        postgres_url_parts._replace(
            netloc=f"{pg_user}:{pg_pass}@{postgres_url_parts.hostname}{f":{postgres_url_parts.port}" if postgres_url_parts.port is not None else ""}",
            path=f"/{pg_db}",
        )
    )
    s3_bucket_parts = urlparse(args.s3_bucket)
    persist_backend_url = urlunparse(
        s3_bucket_parts._replace(
            path=f"{s3_bucket_parts.path}/{args.environment_id}",
        )
    )

    secret_name = f"materialize-backend-{args.environment_id}"

    resources = dedent(
        f"""
        ---
        apiVersion: v1
        kind: Secret
        metadata:
          name: {secret_name}
        stringData:
          metadata_backend_url: {metadata_backend_url}
          persist_backend_url: {persist_backend_url}
        ---
        apiVersion: materialize.cloud/v1alpha1
        kind: Materialize
        metadata:
          name: {args.environment_id}
        spec:
          environmentdImageRef: {environmentd_image_ref}
          backendSecretName: {secret_name}
        """
    )
    env_kubectl("apply", "-f", "-", input=resources.encode())

    resource_id = None
    for _ in range(60):
        try:
            resource_id = (
                env_kubectl(
                    "get",
                    "materialize",
                    args.environment_id,
                    "-o=jsonpath={.status.resourceId}",
                )
                .decode()
                .strip()
            )
            if resource_id == "<no value>" or resource_id == "":
                resource_id = None
        except subprocess.CalledProcessError:
            resource_id = None

        if resource_id is not None:
            break

        sleep(1)
    assert resource_id is not None

    node_port = None
    for _ in range(60):
        try:
            node_port = (
                env_kubectl(
                    "get",
                    "service",
                    f"mz{resource_id}-console",
                    '-o=jsonpath={.spec.ports[?(@.name=="http")].nodePort}',
                )
                .decode()
                .strip()
            )
            if node_port == "<no value>" or node_port == "":
                node_port = None
        except subprocess.CalledProcessError:
            node_port = None

        if node_port is not None:
            break

        sleep(1)
    assert node_port is not None

    env_kubectl(
        "wait",
        "--for=condition=Available",
        f"deployment/mz{resource_id}-console",
    )

    console_url = f"http://localhost:{node_port}"
    print(console_url)
    if shutil.which("open"):
        subprocess.check_call(["open", console_url])
    elif shutil.which("xdg-open"):
        subprocess.check_call(["xdg-open", console_url])


def acquire(image: str, dev: bool, cluster: str):
    args = []
    if dev:
        args.append("--dev")
    subprocess.check_call(["bin/mzimage", "acquire", image, *args])
    fingerprint = (
        subprocess.check_output(
            [
                "bin/mzimage",
                "fingerprint",
                image,
                *args,
            ]
        )
        .decode()
        .strip()
    )
    subprocess.check_call(
        [
            "docker",
            "tag",
            f"materialize/{image}:mzbuild-{fingerprint}",
            f"materialize/{image}:{DEV_IMAGE_TAG}",
        ]
    )
    kind(
        "load",
        "docker-image",
        f"materialize/{image}:{DEV_IMAGE_TAG}",
        cluster=cluster,
    )


def kind(*args: str, cluster: str, **subprocess_args):
    return subprocess.check_output(
        ["kind", "--name", cluster, *args],
        **subprocess_args,
    )


def kubectl(*args: str, cluster: str, **subprocess_args):
    return subprocess.check_output(
        ["kubectl", "--context", f"kind-{cluster}", *args],
        **subprocess_args,
    )


if __name__ == "__main__":
    with ui.error_handler("orchestratord"):
        main()
