# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import yaml

from materialize import MZ_ROOT
from materialize.cloudtest import DEFAULT_K8S_NAMESPACE
from materialize.cloudtest.k8s.api.k8s_resource import K8sResource

# Vendored from
# https://github.com/kubernetes/examples/tree/1b8cbf894ead6b25e9e870af6ae04f49dfdedfc9/staging/storage/minio
# rather than fetched at runtime. Fetching from raw.githubusercontent.com on
# every application boot intermittently trips GitHub's per-IP rate limit (HTTP
# 429), which failed cloudtest setup.
MINIO_YAML_DIRECTORY = MZ_ROOT / "misc/python/materialize/cloudtest/k8s/minio-yaml"


class Minio(K8sResource):
    def __init__(
        self, namespace: str = DEFAULT_K8S_NAMESPACE, apply_node_selectors: bool = False
    ) -> None:
        super().__init__(namespace)
        self.apply_node_selectors = apply_node_selectors

    def create(self) -> None:
        self.kubectl(
            "delete",
            "persistentvolumeclaim",
            "minio-pv-claim",
            "--ignore-not-found",
            "true",
        )

        for yaml_file in [
            "minio-standalone-pvc",
            "minio-standalone-service",
        ]:
            self.kubectl(
                "create",
                "-f",
                str(MINIO_YAML_DIRECTORY / f"{yaml_file}.yaml"),
            )

        # NOTE: The deployment must carry its final nodeSelector before it is
        # created, so it is injected here rather than patched in afterwards.
        # The claim's storage class binds with WaitForFirstConsumer, so the
        # provisioner pins the volume to whichever node the scheduler picks for
        # the first pod that consumes the claim. A pod created without the
        # nodeSelector can drive that decision even if it is replaced moments
        # later, pinning the volume to a node the final pod may not run on. The
        # pod then stays Pending forever, because nothing can satisfy both the
        # volume's node affinity and the pod's node selector.
        self.kubectl(
            "create",
            "-f",
            "-",
            input=self.deployment_manifest(),
        )

        self.wait(
            resource="deployment.apps/minio-deployment",
            condition="condition=Available=True",
            timeout_secs=600,
        )

        self.create_buckets(["persist", "copytos3", "copyfroms3"])

    def deployment_manifest(self) -> str:
        with open(MINIO_YAML_DIRECTORY / "minio-standalone-deployment.yaml") as f:
            deployment = yaml.safe_load(f)

        if self.apply_node_selectors:
            deployment["spec"]["template"]["spec"]["nodeSelector"] = {
                "supporting-services": "true"
            }

        return yaml.dump(deployment)

    def create_buckets(self, buckets: list[str]) -> None:
        cmds = [
            f"mc config host add myminio http://minio-service.{self.namespace()}:9000 minio minio123"
        ]
        for bucket in buckets:
            cmds.extend(
                [
                    f"mc rm -r --force myminio/{bucket}",
                    f"mc mb myminio/{bucket}",
                ]
            )
        self.kubectl(
            "run",
            "minio",
            "--image=minio/mc:RELEASE.2023-07-07T05-25-51Z",
            "--restart=Never",
            "--command",
            "/bin/sh",
            "--",
            "-c",
            ";".join(cmds),
        )

        self.wait(
            resource="pod/minio",
            condition="jsonpath={.status.containerStatuses[0].state.terminated.reason}=Completed",
        )
