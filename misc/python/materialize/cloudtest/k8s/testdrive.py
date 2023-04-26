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
from typing import Optional

from kubernetes.client import V1Container, V1EnvVar, V1ObjectMeta, V1Pod, V1PodSpec

from materialize.cloudtest.k8s import K8sPod
from materialize.cloudtest.wait import wait


class Testdrive(K8sPod):
    def __init__(
        self, release_mode: bool, coverage: bool, aws_region: Optional[str] = None
    ) -> None:
        self.aws_region = aws_region

        metadata = V1ObjectMeta(name="testdrive")

        # Pass through AWS credentials from the host
        env = [
            V1EnvVar(name=var, value=os.environ.get(var))
            for var in [
                "AWS_ACCESS_KEY_ID",
                "AWS_SECRET_ACCESS_KEY",
                "AWS_SESSION_TOKEN",
            ]
        ]

        container = V1Container(
            name="testdrive",
            # NOTE(benesch): is it intentional that this does not pass through
            # the tag from the caller?
            image=self.image(
                "testdrive", tag=None, release_mode=release_mode, coverage=coverage
            ),
            command=["sleep", "infinity"],
            env=env,
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
                "--var=replicas=1",
                "--var=default-storage-size=1",
                "--var=default-replica-size=1",
                *([f"--aws-region={self.aws_region}"] if self.aws_region else []),
                # S3 sources are not compatible with Minio unfortunately
                # "--aws-endpoint=http://minio-service.default:9000",
                # "--aws-access-key-id=minio",
                # "--aws-secret-access-key=minio123",
                *(["--no-reset"] if no_reset else []),
                *([f"--seed={seed}"] if seed else []),
                *args,
            ],
            check=True,
            input=input,
            text=True,
        )
