# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from kubernetes.client import (
    V1Container,
    V1ContainerPort,
    V1Deployment,
    V1DeploymentSpec,
    V1EnvVar,
    V1LabelSelector,
    V1ObjectMeta,
    V1PodSpec,
    V1PodTemplateSpec,
    V1Service,
    V1ServicePort,
    V1ServiceSpec,
)

from materialize.cloudtest.k8s import K8sDeployment, K8sService


class PostgresSourceService(K8sService):
    def __init__(self) -> None:
        service_port = V1ServicePort(name="sql", port=5432)

        self.service = V1Service(
            api_version="v1",
            kind="Service",
            metadata=V1ObjectMeta(
                name="postgres-source", labels={"app": "postgres-source"}
            ),
            spec=V1ServiceSpec(
                type="NodePort",
                ports=[service_port],
                selector={"app": "postgres-source"},
            ),
        )


class PostgresSourceDeployment(K8sDeployment):
    def __init__(self) -> None:
        env = [
            V1EnvVar(name="POSTGRESDB", value="postgres"),
            V1EnvVar(name="POSTGRES_PASSWORD", value="postgres"),
        ]
        ports = [V1ContainerPort(container_port=5432, name="sql")]
        container = V1Container(
            name="postgres-source",
            image=self.image("postgres", True),
            args=["-c", "wal_level=logical"],
            env=env,
            ports=ports,
        )

        template = V1PodTemplateSpec(
            metadata=V1ObjectMeta(labels={"app": "postgres-source"}),
            spec=V1PodSpec(containers=[container]),
        )

        selector = V1LabelSelector(match_labels={"app": "postgres-source"})

        spec = V1DeploymentSpec(replicas=1, template=template, selector=selector)

        self.deployment = V1Deployment(
            api_version="apps/v1",
            kind="Deployment",
            metadata=V1ObjectMeta(name="postgres-source"),
            spec=spec,
        )


POSTGRES_SOURCE_RESOURCES = [
    PostgresSourceService(),
    PostgresSourceDeployment(),
]
