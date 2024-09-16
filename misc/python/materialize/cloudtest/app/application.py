# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import os
import subprocess

from materialize import ui
from materialize.cloudtest import DEFAULT_K8S_CLUSTER_NAME, DEFAULT_K8S_CONTEXT_NAME
from materialize.cloudtest.k8s.api.k8s_resource import K8sResource
from materialize.cloudtest.util.common import log_subprocess_error


class Application:
    resources: list[K8sResource]
    images: list[str]
    release_mode: bool
    aws_region: str | None

    def __init__(self) -> None:
        pass

    def create_resources(self) -> None:
        self.acquire_images()
        for resource in self.resources:
            resource.create()

    def coverage_mode(self) -> bool:
        return ui.env_is_truthy("CI_COVERAGE_ENABLED")

    def sanitizer_mode(self) -> str:
        return os.getenv("CI_SANITIZER", "none")

    def bazel(self) -> bool:
        return ui.env_is_truthy("CI_BAZEL_BUILD")

    def bazel_remote_cache(self) -> str | None:
        return os.getenv("CI_BAZEL_REMOTE_CACHE")

    def acquire_images(self) -> None:
        raise NotImplementedError

    def kubectl(self, *args: str, namespace: str | None = None) -> str:
        try:
            cmd = ["kubectl", "--context", self.context(), *args]

            if namespace is not None:
                cmd.extend(["--namespace", namespace])

            return subprocess.check_output(cmd, text=True)
        except subprocess.CalledProcessError as e:
            log_subprocess_error(e)
            raise e

    def context(self) -> str:
        return DEFAULT_K8S_CONTEXT_NAME

    def cluster_name(self) -> str:
        return DEFAULT_K8S_CLUSTER_NAME
