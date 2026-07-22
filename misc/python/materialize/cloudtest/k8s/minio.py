# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

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

        # the PVC will be created afterwards
        for yaml_file in [
            "minio-standalone-deployment",
            "minio-standalone-service",
        ]:
            self.kubectl(
                "create",
                "-f",
                str(MINIO_YAML_DIRECTORY / f"{yaml_file}.yaml"),
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
            str(MINIO_YAML_DIRECTORY / "minio-standalone-pvc.yaml"),
        )

        self.wait(
            resource="deployment.apps/minio-deployment",
            condition="condition=Available=True",
            timeout_secs=600,
        )

        self.create_buckets(["persist", "copytos3", "copyfroms3"])

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
