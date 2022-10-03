# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import subprocess
from typing import Optional

from kubernetes.client import V1Container, V1ObjectMeta, V1Pod, V1PodSpec

from materialize.cloudtest.k8s import K8sPod
from materialize.cloudtest.wait import wait


class Testdrive(K8sPod):
    def __init__(self, release_mode: bool) -> None:
        metadata = V1ObjectMeta(name="testdrive")

        container = V1Container(
            name="testdrive",
            image=self.image("testdrive", release_mode),
            command=["sleep", "infinity"],
        )

        pod_spec = V1PodSpec(containers=[container])
        self.pod = V1Pod(metadata=metadata, spec=pod_spec)

    def run(
        self,
        *args: str,
        input: Optional[str] = None,
        no_reset: bool = False,
        seed: Optional[int] = None,
    ) -> None:
        wait(condition="condition=Ready", resource="pod/testdrive")
        subprocess.run(
            [
                "kubectl",
                "--context",
                self.context(),
                "exec",
                "-it",
                "testdrive",
                "--",
                "testdrive",
                "--materialize-url=postgres://materialize:materialize@environmentd:6875/materialize",
                "--materialize-internal-url=postgres://materialize:materialize@environmentd:6877/materialize",
                "--kafka-addr=redpanda:9092",
                "--schema-registry-url=http://redpanda:8081",
                "--default-timeout=300s",
                "--aws-endpoint=http://minio-service.default:9000",
                "--aws-access-key-id=minio",
                "--aws-secret-access-key=minio123",
                *(["--no-reset"] if no_reset else []),
                *([f"--seed={seed}"] if seed else []),
                *args,
            ],
            check=True,
            input=input,
            text=True,
        )
