# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from textwrap import dedent

from kubernetes.client import (
    V1ConfigMap,
    V1ConfigMapVolumeSource,
    V1Container,
    V1ContainerPort,
    V1EnvVar,
    V1LabelSelector,
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
    V1Volume,
    V1VolumeMount,
)

from materialize.cloudtest.k8s import K8sConfigMap, K8sService, K8sStatefulSet


class PostgresConfigMap(K8sConfigMap):
    def __init__(self) -> None:
        self.config_map = V1ConfigMap(
            metadata=V1ObjectMeta(
                name="postgres-init",
            ),
            data={
                "connections.sql": dedent(
                    """
                    ALTER SYSTEM SET max_connections = 5000;
                    """
                ),
                "schemas.sql": dedent(
                    """
                CREATE SCHEMA consensus;
                CREATE SCHEMA catalog;
                CREATE SCHEMA storage;
            """
                ),
            },
        )


class PostgresService(K8sService):
    def __init__(self) -> None:
        service_port = V1ServicePort(name="sql", port=5432)

        self.service = V1Service(
            api_version="v1",
            kind="Service",
            metadata=V1ObjectMeta(name="postgres", labels={"app": "postgres"}),
            spec=V1ServiceSpec(
                type="NodePort", ports=[service_port], selector={"app": "postgres"}
            ),
        )


class PostgresStatefulSet(K8sStatefulSet):
    def __init__(self) -> None:
        metadata = V1ObjectMeta(name="postgres", labels={"app": "postgres"})
        label_selector = V1LabelSelector(match_labels={"app": "postgres"})
        env = [V1EnvVar(name="POSTGRES_HOST_AUTH_METHOD", value="trust")]
        ports = [V1ContainerPort(container_port=5432, name="sql")]
        volume_mounts = [
            V1VolumeMount(name="data", mount_path="/data"),
            V1VolumeMount(
                name="postgres-init", mount_path="/docker-entrypoint-initdb.d"
            ),
        ]

        volume_config = V1ConfigMapVolumeSource(
            name="postgres-init",
        )

        volumes = [V1Volume(name="postgres-init", config_map=volume_config)]

        container = V1Container(
            name="postgres",
            image="postgres:14.3",
            env=env,
            ports=ports,
            volume_mounts=volume_mounts,
        )

        pod_spec = V1PodSpec(containers=[container], volumes=volumes)
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
                service_name="postgres",
                replicas=1,
                selector=label_selector,
                template=template_spec,
                volume_claim_templates=claim_templates,
            ),
        )


POSTGRES_RESOURCES = [
    PostgresConfigMap(),
    PostgresService(),
    PostgresStatefulSet(),
]
