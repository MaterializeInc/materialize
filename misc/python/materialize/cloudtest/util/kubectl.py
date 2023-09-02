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
from typing import Any

import yaml

from materialize.cloudtest.util.common import retry
from materialize.cloudtest.util.wait import wait


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
                f"stdout: {self.stdout.decode('utf-8')}",
                f"stderr: {self.stderr.decode('utf-8')}",
            ],
        )


class Kubectl:
    def __init__(self, context: str):
        self.context = context

    def patch(
        self,
        resource_type: str,
        name: str,
        namespace: str | None,
        patch: Any,
    ) -> None:
        command = [
            "kubectl",
            "--context",
            self.context,
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

    def wait(
        self,
        namespace: str | None,
        resource_type: str,
        resource_name: str | None,
        wait_for: str,
        timeout_secs: int,
        label: str | None = None,
    ) -> None:
        if resource_name is None and label is None:
            raise RuntimeError("Either resource_name or label must be set")

        if resource_name is None:
            resource = resource_type
        else:
            resource = f"{resource_type}/{resource_name}"

        wait(
            wait_for,
            resource,
            timeout_secs,
            self.context,
            label=label,
            namespace=namespace,
        )

    def delete(
        self,
        namespace: str | None,
        resource_type: str,
        resource_name: str,
    ) -> None:
        command = [
            "kubectl",
            "--context",
            self.context,
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

    def get(
        self,
        namespace: str | None,
        resource_type: str,
        resource_name: str | None = None,
    ) -> dict[str, Any]:
        command = [
            "kubectl",
            "--context",
            self.context,
            "get",
            resource_type,
        ]
        if resource_name is not None:
            command.append(resource_name)
        if namespace:
            command.extend(["-n", namespace])

        command.extend(["-o", "yaml"])

        try:
            yaml_data: dict[str, Any] = yaml.safe_load(
                subprocess.run(
                    command,
                    capture_output=True,
                    check=True,
                ).stdout,
            )
            return yaml_data
        except subprocess.CalledProcessError as e:
            raise KubectlError.from_subprocess_error(e) from e

    def get_retry(
        self,
        namespace: str | None,
        resource_type: str,
        resource_name: str,
        max_attempts: int,
    ) -> dict[str, Any]:
        def f() -> dict[str, Any]:
            return self.get(namespace, resource_type, resource_name)

        yaml_data: dict[str, Any] = retry(
            f,
            max_attempts=max_attempts,
            exception_types=[KubectlError],
        )
        return yaml_data

    def get_or_none(
        self,
        namespace: str | None,
        resource_type: str,
        resource_name: str | None = None,
    ) -> dict[str, Any] | None:
        try:
            return self.get(namespace, resource_type, resource_name)
        except KubectlError as e:
            if b"NotFound" in e.stderr:
                return None
            raise

    def load_k8s_yaml(
        self,
        filepath: str,
        tests_dir: str,
        substitutions: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """
        Load a Kubernetes YAML specification to assert against. If `substitutions`
        are given, find-and-replace in the YAML contents before parsing.
        """
        contents = Path(tests_dir).joinpath(filepath).read_text()
        for old, new in (substitutions or {}).items():
            contents = contents.replace(old, new)
        yaml_data: dict[str, Any] = yaml.safe_load(contents)
        return yaml_data
