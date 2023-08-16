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

from materialize.cloudtest.util.common import eprint, retry


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
        namespace: Optional[str],
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
        namespace: Optional[str],
        resource_type: str,
        resource_name: str,
        wait_for: str,
        timeout_secs: int,
    ) -> None:
        command = [
            "kubectl",
            "--context",
            self.context,
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

    def delete(
        self,
        namespace: Optional[str],
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
        namespace: Optional[str],
        resource_type: str,
        resource_name: Optional[str] = None,
    ) -> Dict[str, Any]:
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
            yaml_data: Dict[str, Any] = yaml.safe_load(
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
        namespace: Optional[str],
        resource_type: str,
        resource_name: str,
        max_attempts: int,
    ) -> Dict[str, Any]:
        def f() -> Dict[str, Any]:
            return self.get(namespace, resource_type, resource_name)

        yaml_data: Dict[str, Any] = retry(
            f,
            max_attempts=max_attempts,
            exception_types=[KubectlError],
        )
        return yaml_data

    def get_or_none(
        self,
        namespace: Optional[str],
        resource_type: str,
        resource_name: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        try:
            return self.get(namespace, resource_type, resource_name)
        except KubectlError as e:
            if b"NotFound" in e.stderr:
                return None
            raise

    def load_k8s_yaml(
        self,
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
        yaml_data: Dict[str, Any] = yaml.safe_load(contents)
        return yaml_data
