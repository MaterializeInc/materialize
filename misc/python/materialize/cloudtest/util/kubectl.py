# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import json
import subprocess
from pathlib import Path
from typing import Any, Dict, Optional

import yaml

from .common import eprint, retry


class KubectlError(AssertionError):
    def __init__(self, returncode: int, cmd: list[str], stdout: bytes, stderr: bytes):
        self.returncode = returncode
        self.cmd = cmd
        self.stdout = stdout
        self.stderr = stderr

    @classmethod
    def from_subprocess_error(cls, e: subprocess.CalledProcessError) -> BaseException:
        return cls(e.returncode, e.cmd, e.stdout, e.stderr)

    def __str__(self) -> str:
        return "\n".join(
            [
                f"error in kubectl command: {self.cmd} returned {self.returncode}",
                f"stdout: {self.stdout}",
                f"stderr: {self.stderr}",
            ],
        )


def kubectl_patch(
    context: str,
    resource_type: str,
    name: str,
    namespace: Optional[str],
    patch: Any,
) -> None:
    command = [
        "kubectl",
        "--context",
        context,
        "patch",
        resource_type,
        name,
        "-p",
        json.dumps(patch),
        "--type",
        "merge",
    ]
    if namespace:
        command.extend(["-n", namespace])
    subprocess.run(
        command,
        check=True,
    )


def kubectl_wait(
    context: str,
    namespace: Optional[str],
    resource_type: str,
    resource_name: str,
    wait_for: str,
    timeout_secs: int,
) -> None:
    command = [
        "kubectl",
        "--context",
        context,
        "wait",
        f"{resource_type}/{resource_name}",
        "--for",
        wait_for,
        "--timeout",
        f"{timeout_secs}s",
    ]
    if namespace:
        command.extend(["-n", namespace])
    eprint(f"Waiting for {resource_type} {resource_name} to be {wait_for}")
    subprocess.run(
        command,
        check=True,
    )


def kubectl_delete(
    context: str,
    namespace: Optional[str],
    resource_type: str,
    resource_name: str,
) -> None:
    command = [
        "kubectl",
        "--context",
        context,
        "delete",
        resource_type,
        resource_name,
        "--wait=true",
        "--cascade=foreground",
    ]
    if namespace:
        command.extend(["-n", namespace])

    try:
        subprocess.run(
            command,
            capture_output=True,
            check=True,
            text=True,
        )
    except subprocess.CalledProcessError as e:
        if "NotFound" not in e.stderr:
            raise KubectlError.from_subprocess_error(e) from e


def kubectl_get(
    context: str,
    namespace: Optional[str],
    resource_type: str,
    resource_name: Optional[str] = None,
) -> Any:
    command = [
        "kubectl",
        "--context",
        context,
        "get",
        resource_type,
    ]
    if resource_name is not None:
        command.append(resource_name)
    if namespace:
        command.extend(["-n", namespace])

    command.extend(["-o", "yaml"])

    try:
        return yaml.safe_load(
            subprocess.run(
                command,
                capture_output=True,
                check=True,
            ).stdout,
        )
    except subprocess.CalledProcessError as e:
        raise KubectlError.from_subprocess_error(e) from e


def kubectl_get_retry(
    context: str,
    namespace: Optional[str],
    resource_type: str,
    resource_name: str,
    max_attempts: int,
) -> Dict[str, Any]:
    def f():
        return kubectl_get(context, namespace, resource_type, resource_name)

    return retry(
        f,
        max_attempts=max_attempts,
        exception_types=[KubectlError],
    )


def kubectl_get_or_none(
    context: str,
    namespace: Optional[str],
    resource_type: str,
    resource_name: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    try:
        return kubectl_get(context, namespace, resource_type, resource_name)
    except KubectlError as e:
        if b"NotFound" in e.stderr:
            return None
        raise


def load_k8s_yaml(
    filepath: str,
    tests_dir: Optional[str] = None,
    substitutions: Optional[dict[str, str]] = None,
) -> Dict[str, Any]:
    """
    Load a Kubernetes YAML specification to assert against. If `substitutions`
    are given, find-and-replace in the YAML contents before parsing.
    """
    # TODO(necaris): Make this path-finding less fragile
    if tests_dir is None:
        tests_dir = str(Path(__file__).parent.parent)
    contents = Path(tests_dir).joinpath(filepath).read_text()
    for old, new in (substitutions or {}).items():
        contents = contents.replace(old, new)
    return yaml.safe_load(contents)


def await_environment_pod(context: str, namespace: str, pod_name: str) -> None:
    # we can't just wait, since it errors if it doesn't exist yet
    kubectl_get_retry(
        context=context,
        namespace=namespace,
        resource_type="pod",
        resource_name=pod_name,
        # Especially on dev stacks, this can take a little while
        max_attempts=180,
    )
    kubectl_wait(
        context=context,
        namespace=namespace,
        resource_type="pod",
        resource_name=pod_name,
        wait_for="condition=Ready=True",
        # If we're unlucky with certificates, we can take up to 10 minutes :-(
        # -- pad a bit just in case
        timeout_secs=630,
    )


def cleanup_crds(
    system_context: str,
    environment_context: str,
) -> None:
    if system_context != "kind-mzcloud":
        return

    assert "production" not in system_context
    assert "production" not in environment_context
    assert "staging" not in system_context
    assert "staging" not in environment_context

    kubectl_delete(
        context=system_context,
        namespace=None,
        resource_type="crd",
        resource_name="environmentassignments.materialize.cloud",
    )

    kubectl_delete(
        context=environment_context,
        namespace=None,
        resource_type="crd",
        resource_name="environments.materialize.cloud",
    )

    kubectl_delete(
        context=environment_context,
        namespace=None,
        resource_type="crd",
        resource_name="vpcendpoints.materialize.cloud",
    )
