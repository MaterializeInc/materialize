# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import subprocess
from textwrap import dedent
from typing import List, Optional

from materialize import ui
from materialize.cloudtest.k8s import K8sResource


class Application:
    resources: List[K8sResource]
    images: List[str]
    release_mode: bool
    aws_region: Optional[str]

    def __init__(self) -> None:
        self.create()

    def create(self) -> None:
        self.acquire_images()
        for resource in self.resources:
            resource.create()

    def coverage_mode(self) -> bool:
        return ui.env_is_truthy("CI_COVERAGE_ENABLED")

    def acquire_images(self) -> None:
        raise NotImplementedError

    def kubectl(self, *args: str) -> str:
        try:
            return subprocess.check_output(
                ["kubectl", "--context", self.context(), *args], text=True
            )
        except subprocess.CalledProcessError as e:
            print(
                dedent(
                    f"""
                    cmd: {e.cmd}
                    returncode: {e.returncode}
                    stdout: {e.stdout}
                    stderr: {e.stderr}
                    """
                )
            )
            raise e

    def context(self) -> str:
        return "kind-cloudtest"
