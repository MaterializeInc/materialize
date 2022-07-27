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
from typing import List
from unittest.mock import patch

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
        # Direct mzbuild to push the images into minikube's container registry
        # while preserving the original values for use inside the ci-builder
        minikube_env = {
            "MZ_DEV_CI_BUILDER_DOCKER_HOST": os.environ.get("DOCKER_HOST", ""),
            "MZ_DEV_CI_BUILDER_DOCKER_TLS_VERIFY": os.environ.get(
                "DOCKER_TLS_VERIFY", ""
            ),
            "MZ_DEV_CI_BUILDER_DOCKER_CERT_PATH": os.environ.get(
                "DOCKER_CERT_PATH", ""
            ),
        }

        minikube_env_str = subprocess.check_output(["minikube", "docker-env"]).decode(
            "ascii"
        )
        prefix = "export "
        for minikube_env_line in minikube_env_str.splitlines():
            if minikube_env_line.startswith(prefix):
                parts = minikube_env_line[len(prefix) :].split("=")
                minikube_env[parts[0]] = parts[1].strip('"')

        repo = mzbuild.Repository(ROOT)
        with patch.dict("os.environ", minikube_env):
            for image in self.images:
                deps = repo.resolve_dependencies([repo.images[image]])
                deps.acquire()


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

        # Label the minicube node in a way that mimics Materialize cloud
        subprocess.check_call(
            [
                "kubectl",
                "label",
                "--overwrite",
                "node/minikube",
                "materialize.cloud/availability-zone=",
            ]
        )

        super().__init__()

    def create(self) -> None:
        super().create()
        wait(condition="condition=Ready", resource="pod/compute-cluster-1-replica-1-0")
