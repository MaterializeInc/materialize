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


class CockroachConfigMap(K8sConfigMap):
    def __init__(self) -> None:
        self.config_map = V1ConfigMap(
            metadata=V1ObjectMeta(
                name="cockroach-init",
            ),
            data={
                "schemas.sql": dedent(
                    """
                CREATE SCHEMA consensus;
                CREATE SCHEMA catalog;
                CREATE SCHEMA storage;
                """
                ),
            },
        )


class CockroachService(K8sService):
    def __init__(self) -> None:
        service_port = V1ServicePort(name="sql", port=26257)

        self.service = V1Service(
            api_version="v1",
            kind="Service",
            metadata=V1ObjectMeta(name="cockroach", labels={"app": "cockroach"}),
            spec=V1ServiceSpec(
                type="NodePort", ports=[service_port], selector={"app": "cockroach"}
            ),
        )


class CockroachStatefulSet(K8sStatefulSet):
    def generate_stateful_set(self) -> V1StatefulSet:
        metadata = V1ObjectMeta(name="cockroach", labels={"app": "cockroach"})
        label_selector = V1LabelSelector(match_labels={"app": "cockroach"})
        ports = [V1ContainerPort(container_port=26257, name="sql")]
        volume_mounts = [
            V1VolumeMount(name="data", mount_path="/cockroach/cockroach-data"),
            V1VolumeMount(
                name="cockroach-init", mount_path="/docker-entrypoint-initdb.d"
            ),
        ]

        volume_config = V1ConfigMapVolumeSource(
            name="cockroach-init",
        )

        volumes = [V1Volume(name="cockroach-init", config_map=volume_config)]

        container = V1Container(
            name="cockroach",
            image="cockroachdb/cockroach:v22.2.0",
            args=["start-single-node", "--insecure"],
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

        return V1StatefulSet(
            api_version="apps/v1",
            kind="StatefulSet",
            metadata=metadata,
            spec=V1StatefulSetSpec(
                service_name="cockroach",
                replicas=1,
                selector=label_selector,
                template=template_spec,
                volume_claim_templates=claim_templates,
            ),
        )


COCKROACH_RESOURCES = [
    CockroachConfigMap(),
    CockroachService(),
    CockroachStatefulSet(),
]
