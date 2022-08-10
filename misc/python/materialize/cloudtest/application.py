# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import subprocess
from typing import List

from materialize import ROOT, mzbuild
from materialize.cloudtest.k8s import K8sResource
from materialize.cloudtest.k8s.environmentd import (
    EnvironmentdService,
    EnvironmentdStatefulSet,
)
from materialize.cloudtest.k8s.minio import Minio
from materialize.cloudtest.k8s.postgres import POSTGRES_RESOURCES
from materialize.cloudtest.k8s.redpanda import REDPANDA_RESOURCES
from materialize.cloudtest.k8s.role_binding import AdminRoleBinding
from materialize.cloudtest.k8s.testdrive import Testdrive
from materialize.cloudtest.wait import wait


class Application:
    resources: List[K8sResource]
    images: List[str]

    def __init__(self) -> None:
        self.create()

    def create(self) -> None:
        self.acquire_images()
        for resource in self.resources:
            resource.create()

    def acquire_images(self) -> None:
        repo = mzbuild.Repository(ROOT)
        for image in self.images:
            deps = repo.resolve_dependencies([repo.images[image]])
            deps.acquire()
            for dep in deps:
                subprocess.check_call(
                    [
                        "kind",
                        "load",
                        "docker-image",
                        dep.spec(),
                    ]
                )

    def kubectl(self, *args: str) -> str:
        return subprocess.check_output(
            ["kubectl", "--context", self.context(), *args]
        ).decode("ascii")

    def context(self) -> str:
        return "kind-kind"


class MaterializeApplication(Application):
    def __init__(self) -> None:
        self.environmentd = EnvironmentdService()
        self.testdrive = Testdrive()

        self.resources = [
            *POSTGRES_RESOURCES,
            *REDPANDA_RESOURCES,
            Minio(),
            AdminRoleBinding(),
            EnvironmentdStatefulSet(),
            self.environmentd,
            self.testdrive,
        ]

        self.images = ["environmentd", "computed", "storaged", "testdrive"]

        # Label the minicube nodes in a way that mimics Materialize cloud
        for node in ["kind-control-plane", "kind-worker", "kind-worker2"]:
            self.kubectl(
                "label",
                "--overwrite",
                f"node/{node}",
                "materialize.cloud/availability-zone=",
            )

        super().__init__()

    def create(self) -> None:
        super().create()
        wait(condition="condition=Ready", resource="pod/compute-cluster-1-replica-1-0")
