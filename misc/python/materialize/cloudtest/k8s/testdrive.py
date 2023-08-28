# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import os
from inspect import Traceback
from typing import Optional

from kubernetes.client import V1Container, V1EnvVar, V1ObjectMeta, V1Pod, V1PodSpec

from materialize.cloudtest import DEFAULT_K8S_NAMESPACE
from materialize.cloudtest.k8s.api.k8s_pod import K8sPod


class Testdrive(K8sPod):
    def __init__(
        self,
        release_mode: bool,
        aws_region: Optional[str] = None,
        namespace: str = DEFAULT_K8S_NAMESPACE,
        materialize_url: str = "postgres://materialize:materialize@environmentd:6875/materialize",
        materialize_internal_url: str = "postgres://mz_system@environmentd:6877/materialize",
        kafka_addr: str = "redpanda:9092",
        schema_registry_url: str = "http://redpanda:8081",
    ) -> None:
        super().__init__(namespace)
        self.aws_region = aws_region
        self.materialize_url = materialize_url
        self.materialize_internal_url = materialize_internal_url
        self.kafka_addr = kafka_addr
        self.schema_registry_url = schema_registry_url

        metadata = V1ObjectMeta(name="testdrive", namespace=namespace)

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
            image=self.image("testdrive", release_mode=release_mode),
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
        caller: Optional[Traceback] = None,
        default_timeout: str = "300s",
        log_filter: str = "off",
    ) -> None:
        self.wait(condition="condition=Ready", resource="pod/testdrive")
        self.kubectl(
            "exec",
            "-it",
            "testdrive",
            "--",
            "testdrive",
            f"--materialize-url={self.materialize_url}",
            f"--materialize-internal-url={self.materialize_internal_url}",
            f"--kafka-addr={self.kafka_addr}",
            f"--schema-registry-url={self.schema_registry_url}",
            f"--default-timeout={default_timeout}",
            f"--log-filter={log_filter}",
            "--var=replicas=1",
            "--var=default-storage-size=1",
            "--var=default-replica-size=1",
            *([f"--aws-region={self.aws_region}"] if self.aws_region else []),
            # S3 sources are not compatible with Minio unfortunately
            # f"--aws-endpoint=http://minio-service.{self.namespace()}:9000",
            # "--aws-access-key-id=minio",
            # "--aws-secret-access-key=minio123",
            *(["--no-reset"] if no_reset else []),
            *([f"--seed={seed}"] if seed else []),
            *([f"--source={caller.filename}:{caller.lineno}"] if caller else []),
            *args,
            input=input,
        )
