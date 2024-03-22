# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize.cloudtest import DEFAULT_K8S_NAMESPACE
from materialize.cloudtest.k8s.api.k8s_resource import K8sResource

MINIO_YAML_DIRECTORY_URL = (
    "https://raw.githubusercontent.com/kubernetes/examples/master/staging/storage/minio"
)


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

        # the PVC will be created afterwards
        for yaml_file in [
            "minio-standalone-deployment",
            "minio-standalone-service",
        ]:
            self.kubectl(
                "create",
                "-f",
                f"{MINIO_YAML_DIRECTORY_URL}/{yaml_file}.yaml",
            )

        if self.apply_node_selectors:
            self.kubectl(
                "patch",
                "deployment",
                "minio-deployment",
                "--type",
                "json",
                "-p",
                '[{"op": "add", "path": "/spec/template/spec/nodeSelector", "value": {"supporting-services": "true"} }]',
            )

        # the PVC needs to be created after patching the deployment
        self.kubectl(
            "create",
            "-f",
            f"{MINIO_YAML_DIRECTORY_URL}/minio-standalone-pvc.yaml",
        )

        self.wait(
            resource="deployment.apps/minio-deployment",
            condition="condition=Available=True",
        )

        self.create_buckets(["persist", "copytos3"])

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
            "--image=minio/mc",
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
