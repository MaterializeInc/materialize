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

from kubernetes.client import AppsV1Api, CoreV1Api, RbacAuthorizationV1Api
from kubernetes.config import new_client_from_config  # type: ignore

from materialize import MZ_ROOT, mzbuild, ui
from materialize.cloudtest import DEFAULT_K8S_CONTEXT_NAME
from materialize.cloudtest.util.common import run_process_with_error_information
from materialize.cloudtest.util.wait import wait
from materialize.rustc_flags import Sanitizer


class K8sResource:
    def __init__(self, namespace: str):
        self.selected_namespace = namespace

    def kubectl(
        self,
        *args: str,
        input: str | None = None,
        capture_output: bool = False,
        suppress_command_error_output: bool = False,
    ) -> None:
        cmd = [
            "kubectl",
            "--context",
            self.context(),
            "--namespace",
            self.namespace(),
            *args,
        ]

        if suppress_command_error_output:
            subprocess.run(
                cmd, text=True, input=input, check=True, capture_output=capture_output
            )
        else:
            run_process_with_error_information(
                cmd, input, capture_output=capture_output
            )

    def api(self) -> CoreV1Api:
        api_client = new_client_from_config(context=self.context())
        return CoreV1Api(api_client)

    def apps_api(self) -> AppsV1Api:
        api_client = new_client_from_config(context=self.context())
        return AppsV1Api(api_client)

    def rbac_api(self) -> RbacAuthorizationV1Api:
        api_client = new_client_from_config(context=self.context())
        return RbacAuthorizationV1Api(api_client)

    def context(self) -> str:
        return DEFAULT_K8S_CONTEXT_NAME

    def namespace(self) -> str:
        return self.selected_namespace

    def kind(self) -> str:
        raise NotImplementedError

    def create(self) -> None:
        raise NotImplementedError

    def image(
        self,
        service: str,
        tag: str | None = None,
        release_mode: bool = True,
        org: str | None = "materialize",
    ) -> str:
        if tag is not None:
            image_name = f"{service}:{tag}"
            if org is not None:
                image_name = f"{org}/{image_name}"

            return image_name
        else:
            coverage = ui.env_is_truthy("CI_COVERAGE_ENABLED")
            sanitizer = Sanitizer[os.getenv("CI_SANITIZER", "none")]
            bazel = ui.env_is_truthy("CI_BAZEL_BUILD")
            bazel_remote_cache = os.getenv("CI_BAZEL_REMOTE_CACHE")

            repo = mzbuild.Repository(
                MZ_ROOT,
                profile=(
                    mzbuild.Profile.RELEASE if release_mode else mzbuild.Profile.DEV
                ),
                coverage=coverage,
                sanitizer=sanitizer,
                bazel=bazel,
                bazel_remote_cache=bazel_remote_cache,
            )
            deps = repo.resolve_dependencies([repo.images[service]])
            rimage = deps[service]
            return rimage.spec()

    def wait(
        self,
        condition: str,
        resource: str,
    ) -> None:
        wait(condition=condition, resource=resource, namespace=self.selected_namespace)
