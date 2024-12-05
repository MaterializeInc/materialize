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
import json
import os
import socket
import subprocess
import threading
from collections.abc import Callable, Sequence
from textwrap import dedent
from time import sleep
from typing import TypeVar
from urllib.parse import urlparse, urlunparse
from uuid import uuid4

from materialize import MZ_ROOT, ui

DEV_IMAGE_TAG = "local-dev"
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
    subparsers = parser.add_subparsers(required=True)

    parser_run = subparsers.add_parser("run")
    parser_run.add_argument("--dev", action="store_true")
    parser_run.add_argument("--namespace", default="materialize")
    parser_run.set_defaults(func=run)

    parser_reset = subparsers.add_parser("reset")
    parser_reset.add_argument("--namespace", default="materialize")
    parser_reset.set_defaults(func=reset)

    parser_environment = subparsers.add_parser("environment")
    parser_environment.add_argument("--dev", action="store_true")
    parser_environment.add_argument("--namespace", default="materialize")
    parser_environment.add_argument(
        "--environment-name",
        default="12345678-1234-1234-1234-123456789012",
    )
    parser_environment.add_argument("--postgres-url", default=DEFAULT_POSTGRES)
    parser_environment.add_argument("--s3-bucket", default=DEFAULT_MINIO)
    parser_environment.set_defaults(func=environment)

    parser_portforward = subparsers.add_parser("port-forward")
    parser_portforward.add_argument("--namespace", default="materialize")
    parser_portforward.add_argument(
        "--environment-name",
        default="12345678-1234-1234-1234-123456789012",
    )
    parser_portforward.set_defaults(func=portforward)

    args = parser.parse_args()
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
            "--create-namespace",
            f"--namespace={args.namespace}",
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
        (namespace, environment_name) = environment.split("/", 1)
        env_kubectl = make_env_kubectl(args, namespace)

        secret_name = env_kubectl(
            "get",
            "materialize",
            environment_name,
            "-o=jsonpath={.spec.backendSecretName}",
        )
        assert secret_name is not None

        env_kubectl("delete", "--wait=true", "materialize", environment_name)
        env_kubectl("delete", "--wait=true", "secret", secret_name)

    try:
        subprocess.check_call(
            [
                "helm",
                "uninstall",
                "orchestratord",
                f"--namespace={args.namespace}",
            ]
        )
        kubectl(
            "delete",
            "namespace",
            args.namespace,
            cluster=args.kind_cluster_name,
        )
    except subprocess.CalledProcessError:
        pass


def environment(args: argparse.Namespace):
    env_kubectl = make_env_kubectl(args)

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

    environment_id = str(uuid4())

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

    pg_user = f"materialize_{environment_id}"
    pg_pass = "password"
    pg_db = f"materialize_{environment_id}"

    try:
        root_psql(f"""create role "{pg_user}" with nologin""")
        root_psql(f"""alter role "{pg_user}" with login password '{pg_pass}'""")
        root_psql(f"""create database "{pg_db}" with owner "{pg_user}" """)
    except subprocess.CalledProcessError:
        pass

    postgres_url_parts = urlparse(args.postgres_url)
    metadata_backend_url = urlunparse(
        postgres_url_parts._replace(
            netloc=f"{pg_user}:{pg_pass}@{postgres_url_parts.hostname}{f':{postgres_url_parts.port}' if postgres_url_parts.port is not None else ''}",
            path=f"/{pg_db}",
        )
    )
    s3_bucket_parts = urlparse(args.s3_bucket)
    persist_backend_url = urlunparse(
        s3_bucket_parts._replace(
            path=f"{s3_bucket_parts.path}/{environment_id}",
        )
    )

    secret_name = f"materialize-backend-{environment_id}"

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
          name: {args.environment_name}
        spec:
          environmentdImageRef: {environmentd_image_ref}
          backendSecretName: {secret_name}
          environmentId: {environment_id}
        """
    )
    env_kubectl("apply", "-f", "-", input=resources.encode())

    resource_id = get_resource_id(args)
    retry(
        lambda: env_kubectl(
            "wait",
            "--for=condition=Available",
            f"deployment/mz{resource_id}-console",
        ),
        exception_types=(subprocess.CalledProcessError,),
    )


def portforward(args: argparse.Namespace):
    env_kubectl = make_env_kubectl(args)

    resource_id = get_resource_id(args)

    port_forward_targets = {
        "console/http": lambda port: f"http://localhost:{port}/",
        "balancerd/pgwire": lambda port: f"postgres://{os.environ['USER']}@localhost:{port}/materialize",
        "balancerd/http": lambda port: f"http://localhost:{port}/",
        "environmentd/internal-sql": lambda port: f"postgres://mz_system@localhost:{port}/materialize",
        "environmentd/internal-http": lambda port: f"http://localhost:{port}/",
    }

    port_forwards = {}
    sockets = []
    try:
        for port_forward_target in port_forward_targets:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sockets.append(s)
            s.bind(("", 0))
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            port_forwards[port_forward_target] = s.getsockname()[1]
    finally:
        for s in sockets:
            s.close()

    def port_forward(target: str, port: str):
        target_service, target_port = target.split("/")
        # in theory doing a port forward to the service directly should work,
        # but i can't seem to get it to
        selector = json.loads(
            env_kubectl(
                "get",
                "service",
                f"mz{resource_id}-{target_service}",
                "-o=jsonpath={.spec.selector}",
            )
        )
        pod = env_kubectl(
            "get",
            "pod",
            "-o=name",
            *[f"--selector={k}={v}" for k, v in selector.items()],
        ).splitlines()[0]

        env_kubectl(
            "port-forward",
            pod,
            f"{port}:{target_port}",
        )

    threads = [
        threading.Thread(target=port_forward, args=[target, port])
        for target, port in port_forwards.items()
    ]

    for t in threads:
        t.start()
    for target, port in port_forwards.items():
        print(f"{target}: {port_forward_targets[target](port)}")
    for t in threads:
        t.join()


def get_resource_id(args: argparse.Namespace):
    env_kubectl = make_env_kubectl(args)

    def try_get_resource_id():
        resource_id = (
            env_kubectl(
                "get",
                "materialize",
                args.environment_name,
                "-o=jsonpath={.status.resourceId}",
            )
            .decode()
            .strip()
        )
        assert resource_id != ""
        return resource_id

    return retry(
        try_get_resource_id,
        exception_types=(AssertionError, subprocess.CalledProcessError),
    )


def make_env_kubectl(args: argparse.Namespace, namespace: str | None = None):
    def env_kubectl(*cmd_args: str, **subprocess_args):
        return kubectl(
            "-n",
            namespace or args.namespace,
            *cmd_args,
            cluster=args.kind_cluster_name,
            **subprocess_args,
        )

    return env_kubectl


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


T = TypeVar("T")


def retry(
    f: Callable[[], T],
    max_attempts: int = 60,
    sleep_secs: int = 1,
    exception_types: Sequence[type[Exception]] = [AssertionError],
) -> T | None:
    result = None
    for attempt in range(max_attempts):
        try:
            result = f()
            break
        except tuple(exception_types):
            if attempt == max_attempts:
                raise
            sleep(sleep_secs)
    return result


if __name__ == "__main__":
    with ui.error_handler("orchestratord"):
        main()
