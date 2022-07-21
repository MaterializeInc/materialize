# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import subprocess
import urllib.parse
from typing import Any, Tuple

import pg8000
import sqlparse
from kubernetes.client import (
    V1Container,
    V1ContainerPort,
    V1EnvVar,
    V1EnvVarSource,
    V1LabelSelector,
    V1ObjectFieldSelector,
    V1ObjectMeta,
    V1PersistentVolumeClaim,
    V1PersistentVolumeClaimSpec,
    V1PodSpec,
    V1PodTemplateSpec,
    V1ResourceRequirements,
    V1Service,
    V1ServicePort,
    V1ServiceSpec,
    V1StatefulSet,
    V1StatefulSetSpec,
    V1VolumeMount,
)
from pg8000 import Cursor

from materialize.cloudtest.k8s import K8sService, K8sStatefulSet


class EnvironmentdService(K8sService):
    def __init__(self) -> None:
        service_port = V1ServicePort(name="sql", port=6875)
        self.service = V1Service(
            api_version="v1",
            kind="Service",
            metadata=V1ObjectMeta(name="environmentd", labels={"app": "environmentd"}),
            spec=V1ServiceSpec(
                type="NodePort", ports=[service_port], selector={"app": "environmentd"}
            ),
        )

    def host_port(self) -> Tuple[str, str]:
        url = subprocess.check_output(
            ["minikube", "service", "environmentd", "--url"]
        ).decode("ascii")
        parts = str(url).split(":")
        host = parts[1][2:]
        port = parts[2].strip()
        return (host, port)

    def host(self) -> str:
        return self.host_port()[0]

    def port(self) -> int:
        return int(self.host_port()[1])

    def sql_cursor(self) -> Cursor:
        """Get a cursor to run SQL queries against the environmentd stateful set."""
        conn = pg8000.connect(host=self.host(), port=self.port(), user="materialize")
        conn.autocommit = True
        return conn.cursor()

    def sql(self, sql: str) -> None:
        """Run a batch of SQL statements against the environmentd stateful set."""
        with self.sql_cursor() as cursor:
            for statement in sqlparse.split(sql):
                print(f"> {statement}")
                cursor.execute(statement)

    def sql_query(self, sql: str) -> Any:
        """Execute a SQL query against the environmentd statefule set and return results."""
        with self.sql_cursor() as cursor:
            print(f"> {sql}")
            cursor.execute(sql)
            return cursor.fetchall()


class EnvironmentdStatefulSet(K8sStatefulSet):
    def __init__(self) -> None:

        metadata = V1ObjectMeta(name="environmentd", labels={"app": "environmentd"})
        label_selector = V1LabelSelector(match_labels={"app": "environmentd"})

        value_from = V1EnvVarSource(
            field_ref=V1ObjectFieldSelector(field_path="metadata.name")
        )

        env = [
            V1EnvVar(name="MZ_POD_NAME", value_from=value_from),
            V1EnvVar(name="AWS_REGION", value="minio"),
            V1EnvVar(name="AWS_ACCESS_KEY_ID", value="minio"),
            V1EnvVar(name="AWS_SECRET_ACCESS_KEY", value="minio123"),
        ]

        ports = [V1ContainerPort(container_port=5432, name="sql")]

        volume_mounts = [
            V1VolumeMount(name="data", mount_path="/data"),
        ]

        s3_endpoint = urllib.parse.quote("http://minio-service.default:9000")

        container = V1Container(
            name="environmentd",
            image=self.image("environmentd"),
            args=[
                "--storaged-image=" + self.image("storaged"),
                "--computed-image=" + self.image("computed"),
                f"--persist-blob-url=s3://minio:minio123@test/test?endpoint={s3_endpoint}&region=minio",
                "--orchestrator=kubernetes",
                "--orchestrator-kubernetes-image-pull-policy=never",
                "--persist-consensus-url=postgres://postgres@postgres.default?options=--search_path=consensus",
                "--adapter-stash-url=postgres://postgres@postgres.default?options=--search_path=catalog",
                "--storage-stash-url=postgres://postgres@postgres.default?options=--search_path=storage",
                "--unsafe-mode",
            ],
            env=env,
            ports=ports,
            volume_mounts=volume_mounts,
        )

        pod_spec = V1PodSpec(containers=[container])
        template_spec = V1PodTemplateSpec(metadata=metadata, spec=pod_spec)
        claim_templates = [
            V1PersistentVolumeClaim(
                metadata=V1ObjectMeta(name="data"),
                spec=V1PersistentVolumeClaimSpec(
                    access_modes=["ReadWriteOnce"],
                    resources=V1ResourceRequirements(requests={"storage": "1Gi"}),
                ),
            )
        ]

        self.stateful_set = V1StatefulSet(
            api_version="apps/v1",
            kind="StatefulSet",
            metadata=metadata,
            spec=V1StatefulSetSpec(
                service_name="environmentd",
                replicas=1,
                pod_management_policy="Parallel",
                selector=label_selector,
                template=template_spec,
                volume_claim_templates=claim_templates,
            ),
        )
