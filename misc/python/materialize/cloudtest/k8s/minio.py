# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import subprocess

from materialize.cloudtest.k8s import K8sResource
from materialize.cloudtest.wait import wait


class Minio(K8sResource):
    def create(self) -> None:
        # Clear any existing volume claims, but don't crash if we fail to delete
        # because none can be found!
        try:
            subprocess.run(
                ["kubectl", "delete", "persistentvolumeclaim", "minio-pv-claim"],
                check=True,
                capture_output=True,
            )
        except subprocess.CalledProcessError as e:
            if b"(NotFound)" not in e.stderr:
                raise

        for yaml in [
            "minio-standalone-pvc",
            "minio-standalone-deployment",
            "minio-standalone-service",
        ]:
            subprocess.check_call(
                [
                    "kubectl",
                    "create",
                    "-f",
                    f"https://raw.githubusercontent.com/kubernetes/examples/master/staging/storage/minio/{yaml}.yaml",
                ]
            )

        wait(
            resource="deployment.apps/minio-deployment",
            condition="condition=Available=True",
        )

        subprocess.check_call(
            [
                "kubectl",
                "run",
                "minio",
                "--image=minio/mc",
                "--restart=Never",
                "--command",
                "/bin/sh",
                "--",
                "-c",
                ";".join(
                    [
                        "mc config host add myminio http://minio-service.default:9000 minio minio123",
                        "mc rm -r --force myminio/test",
                        "mc mb myminio/test",
                    ]
                ),
            ]
        )
