# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import subprocess

from materialize import MZ_ROOT, mzbuild, ui
from materialize.cloudtest.app.application import Application
from materialize.cloudtest.k8s.api.k8s_resource import K8sResource


class CloudtestApplicationBase(Application):
    def __init__(
        self,
        release_mode: bool = True,
        aws_region: str | None = None,
        log_filter: str | None = None,
    ) -> None:
        super().__init__()
        self.release_mode = release_mode
        self.aws_region = aws_region
        self.mz_root = MZ_ROOT

        self.resources = self.get_resources(log_filter)
        self.images = self.get_images()

    def create_resources_and_wait(self) -> None:
        self.create_resources()
        self.wait_resource_creation_completed()

    def get_resources(self, log_filter: str | None) -> list[K8sResource]:
        raise NotImplementedError

    def get_images(self) -> list[str]:
        raise NotImplementedError

    def wait_resource_creation_completed(self) -> None:
        raise NotImplementedError

    def acquire_images(self) -> None:
        lto = ui.env_is_truthy("CI_LTO")
        repo = mzbuild.Repository(
            self.mz_root,
            profile=(
                (mzbuild.Profile.RELEASE if lto else mzbuild.Profile.OPTIMIZED)
                if self.release_mode
                else mzbuild.Profile.DEV
            ),
            coverage=self.coverage_mode(),
        )
        for image in self.images:
            self._acquire_image(repo, image)

    def _acquire_image(self, repo: mzbuild.Repository, image: str) -> None:
        deps = repo.resolve_dependencies([repo.images[image]])
        deps.acquire()
        for dep in deps:
            subprocess.check_call(
                [
                    "kind",
                    "load",
                    "docker-image",
                    f"--name={self.cluster_name()}",
                    dep.spec(),
                ]
            )
