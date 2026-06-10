# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import json
import os
import subprocess
import sys
from inspect import Traceback

from kubernetes.client import V1Container, V1EnvVar, V1ObjectMeta, V1Pod, V1PodSpec

from materialize.cloudtest import DEFAULT_K8S_NAMESPACE
from materialize.cloudtest.k8s.api.k8s_pod import K8sPod
from materialize.mzcompose import (
    cluster_replica_size_map,
)
from materialize.mzcompose.test_result import (
    extract_error_chunks_from_output,
)
from materialize.ui import CommandFailureCausedUIError
from materialize.util import filter_cmd


class TestdriveBase:
    def __init__(
        self,
        aws_region: str | None = None,
        materialize_url: str | None = None,
        materialize_internal_url: str | None = None,
        kafka_addr: str | None = None,
        schema_registry_url: str | None = None,
    ) -> None:
        self.aws_region = aws_region
        self.materialize_url = (
            materialize_url
            or "postgres://materialize:materialize@environmentd:6875/materialize"
        )
        self.materialize_internal_url = (
            materialize_internal_url
            or "postgres://mz_system@environmentd:6877/materialize"
        )
        self.kafka_addr = kafka_addr or "redpanda:9092"
        self.schema_registry_url = schema_registry_url or "http://redpanda:8081"
        self.aws_endpoint = "http://minio-service.default:9000"

    def run(
        self,
        *args: str,
        input: str | None = None,
        no_reset: bool = False,
        seed: int | None = None,
        caller: Traceback | None = None,
        default_timeout: str = "300s",
        kafka_options: str | None = None,
        log_filter: str = "off",
        suppress_command_error_output: bool = False,
    ) -> None:
        command: list[str] = [
            "testdrive",
            f"--materialize-url={self.materialize_url}",
            f"--materialize-internal-url={self.materialize_internal_url}",
            f"--kafka-addr={self.kafka_addr}",
            f"--schema-registry-url={self.schema_registry_url}",
            f"--default-timeout={default_timeout}",
            f"--log-filter={log_filter}",
            "--var=replicas=1",
            "--var=single-replica-cluster=quickstart",
            "--var=default-storage-size=scale=1,workers=1",
            "--var=default-replica-size=scale=1,workers=1",
            f"--cluster-replica-sizes={json.dumps(cluster_replica_size_map())}",
            *([f"--aws-region={self.aws_region}"] if self.aws_region else []),
            *(
                [
                    f"--aws-endpoint={self.aws_endpoint}",
                    f"--var=aws-endpoint={self.aws_endpoint}",
                    "--aws-access-key-id=minio",
                    "--var=aws-access-key-id=minio",
                    "--aws-secret-access-key=minio123",
                    "--var=aws-secret-access-key=minio123",
                ]
                if not self.aws_region
                else []
            ),
            *(["--no-reset"] if no_reset else []),
            *([f"--seed={seed}"] if seed else []),
            *([f"--source={caller.filename}:{caller.lineno}"] if caller else []),
            *([f"--kafka-option={kafka_options}"] if kafka_options else []),
            *args,
        ]

        self._run_internal(
            command,
            input,
            suppress_command_error_output,
        )

    def _run_internal(
        self,
        command: list[str],
        input: str | None = None,
        suppress_command_error_output: bool = False,
    ) -> None:
        raise NotImplementedError


class TestdrivePod(K8sPod, TestdriveBase):
    def __init__(
        self,
        release_mode: bool,
        aws_region: str | None = None,
        namespace: str = DEFAULT_K8S_NAMESPACE,
        materialize_url: str | None = None,
        materialize_internal_url: str | None = None,
        kafka_addr: str | None = None,
        schema_registry_url: str | None = None,
        apply_node_selectors: bool = False,
    ) -> None:
        K8sPod.__init__(self, namespace)
        TestdriveBase.__init__(
            self,
            aws_region=aws_region,
            materialize_url=materialize_url,
            materialize_internal_url=materialize_internal_url,
            kafka_addr=kafka_addr,
            schema_registry_url=schema_registry_url,
        )

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

        node_selector = None
        if apply_node_selectors:
            node_selector = {"supporting-services": "true"}

        pod_spec = V1PodSpec(containers=[container], node_selector=node_selector)
        self.pod = V1Pod(metadata=metadata, spec=pod_spec)

    def _run_internal(
        self,
        command: list[str],
        input: str | None = None,
        suppress_command_error_output: bool = False,
    ) -> None:
        self.wait(condition="condition=Ready", resource="pod/testdrive")
        try:
            self.kubectl(
                "exec",
                "-it",
                "testdrive",
                "--",
                *command,
                input=input,
                # needed to extract errors
                capture_output=True,
                suppress_command_error_output=suppress_command_error_output,
            )
        except subprocess.CalledProcessError as e:
            if e.stdout is not None:
                print(e.stdout, end="")
            if e.stderr is not None:
                print(e.stderr, file=sys.stderr, end="")

            output = e.stderr or e.stdout
            if output is None and suppress_command_error_output:
                output = "(not captured)"

            assert (
                output is not None
            ), f"Missing stdout and stderr when running '{filter_cmd(e.cmd)}' without success"

            error_chunks = extract_error_chunks_from_output(output)
            error_text = "\n".join(error_chunks)
            raise CommandFailureCausedUIError(
                f"Running {' '.join(filter_cmd(e.cmd))} in testdrive failed with:\n{error_text}",
                filter_cmd(e.cmd),
                e.stdout,
                e.stderr,
            )
