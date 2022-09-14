# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
import random
import string

from materialize.cloudtest.k8s import K8sResource
from materialize.cloudtest.wait import wait


def mc_command(r: K8sResource, *cmds: str) -> str:
    unique_suffix = "".join([random.choice(string.ascii_lowercase) for _ in range(4)])
    pod_name = f"minio-{unique_suffix}"
    res = r.kubectl(
        "run",
        pod_name,
        "--image=minio/mc",
        "--restart=Never",
        "--command",
        "/bin/sh",
        "--",
        "-c",
        ";".join(cmds),
    )
    r.kubectl("delete", "pod", pod_name)
    return res


class Minio(K8sResource):
    def create(self) -> None:
        self.kubectl(
            "delete",
            "persistentvolumeclaim",
            "minio-pv-claim",
            "--ignore-not-found",
            "true",
        )

        for yaml in [
            "minio-standalone-pvc",
            "minio-standalone-deployment",
            "minio-standalone-service",
        ]:
            self.kubectl(
                "create",
                "-f",
                f"https://raw.githubusercontent.com/kubernetes/examples/master/staging/storage/minio/{yaml}.yaml",
            )

        wait(
            resource="deployment.apps/minio-deployment",
            condition="condition=Available=True",
        )

        self.create_bucket("persist")
        # self.create_bucket("usage")

    def mc(self, *cmds: str) -> str:
        return mc_command(self, *cmds)

    def create_bucket(self, bucket: str) -> None:
        self.mc(
            "mc config host add myminio http://minio-service.default:9000 minio minio123",
            # f"mc rm -r --force myminio/{bucket}",
            f"mc mb -p myminio/{bucket}",
        )
