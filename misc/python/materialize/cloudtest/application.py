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
import time
from datetime import datetime, timedelta
from typing import List, Optional

from pg8000.exceptions import InterfaceError

from materialize import ROOT, mzbuild
from materialize.cloudtest.k8s import K8sResource
from materialize.cloudtest.k8s.cockroach import COCKROACH_RESOURCES
from materialize.cloudtest.k8s.debezium import DEBEZIUM_RESOURCES
from materialize.cloudtest.k8s.environmentd import (
    EnvironmentdService,
    EnvironmentdStatefulSet,
)
from materialize.cloudtest.k8s.minio import Minio
from materialize.cloudtest.k8s.postgres_source import POSTGRES_SOURCE_RESOURCES
from materialize.cloudtest.k8s.redpanda import REDPANDA_RESOURCES
from materialize.cloudtest.k8s.role_binding import AdminRoleBinding
from materialize.cloudtest.k8s.ssh import SSH_RESOURCES
from materialize.cloudtest.k8s.testdrive import Testdrive
from materialize.cloudtest.k8s.vpc_endpoints_cluster_role import VpcEndpointsClusterRole
from materialize.cloudtest.wait import wait


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

    def acquire_images(self) -> None:
        repo = mzbuild.Repository(ROOT, release_mode=self.release_mode)
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
    def __init__(
        self,
        release_mode: bool = True,
        tag: Optional[str] = None,
        aws_region: Optional[str] = None,
        log_filter: Optional[str] = None,
    ) -> None:
        self.environmentd = EnvironmentdService()
        self.testdrive = Testdrive(release_mode=release_mode, aws_region=aws_region)
        self.release_mode = release_mode
        self.aws_region = aws_region

        # Register the VpcEndpoint CRD
        self.kubectl(
            "apply",
            "-f",
            os.path.join(
                os.path.abspath(ROOT),
                "src/cloud-resources/src/crd/gen/vpcendpoints.json",
            ),
        )

        # Start metrics-server
        self.kubectl(
            "apply",
            "-f",
            "https://github.com/kubernetes-sigs/metrics-server/releases/download/metrics-server-helm-chart-3.8.2/components.yaml",
        )

        self.kubectl(
            "patch",
            "deployment",
            "metrics-server",
            "--namespace",
            "kube-system",
            "--type",
            "json",
            "-p",
            '[{"op": "add", "path": "/spec/template/spec/containers/0/args/-", "value": "--kubelet-insecure-tls" }]',
        )

        self.resources = [
            *COCKROACH_RESOURCES,
            *POSTGRES_SOURCE_RESOURCES,
            *REDPANDA_RESOURCES,
            *DEBEZIUM_RESOURCES,
            *SSH_RESOURCES,
            Minio(),
            VpcEndpointsClusterRole(),
            AdminRoleBinding(),
            EnvironmentdStatefulSet(
                release_mode=release_mode, tag=tag, log_filter=log_filter
            ),
            self.environmentd,
            self.testdrive,
        ]

        self.images = ["environmentd", "clusterd", "testdrive", "postgres"]

        # Label the minicube nodes in a way that mimics Materialize cloud
        for node in [
            "kind-control-plane",
            "kind-worker",
            "kind-worker2",
            "kind-worker3",
        ]:
            self.kubectl(
                "label",
                "--overwrite",
                f"node/{node}",
                f"materialize.cloud/availability-zone={node}",
            )

        super().__init__()

    def create(self) -> None:
        super().create()
        wait(condition="condition=Ready", resource="pod/compute-cluster-u1-replica-1-0")

    def wait_for_sql(self) -> None:
        """Wait until environmentd pod is ready and can accept SQL connections"""
        wait(condition="condition=Ready", resource="pod/environmentd-0")

        start = datetime.now()
        while datetime.now() - start < timedelta(seconds=300):
            try:
                self.environmentd.sql("SELECT 1")
                break
            except InterfaceError as e:
                # Since we crash environmentd, we expect some errors that we swallow.
                print(f"SQL interface not ready, {e} while SELECT 1. Waiting...")
                time.sleep(2)

    def set_environmentd_failpoints(self, failpoints: str) -> None:
        """Set the FAILPOINTS environmentd variable in the stateful set. This
        will most likely restart environmentd"""
        stateful_set = [
            resource
            for resource in self.resources
            if type(resource) == EnvironmentdStatefulSet
        ]
        assert len(stateful_set) == 1
        stateful_set = stateful_set[0]

        stateful_set.env["FAILPOINTS"] = failpoints
        stateful_set.replace()
        self.wait_for_sql()
