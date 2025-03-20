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
    V1Deployment,
    V1DeploymentSpec,
    V1LabelSelector,
    V1ObjectMeta,
    V1PodSpec,
    V1PodTemplateSpec,
    V1Service,
    V1ServicePort,
    V1ServiceSpec,
)

from materialize.cloudtest import DEFAULT_K8S_NAMESPACE
from materialize.cloudtest.k8s.api.k8s_deployment import K8sDeployment
from materialize.cloudtest.k8s.api.k8s_resource import K8sResource
from materialize.cloudtest.k8s.api.k8s_service import K8sService
from materialize.mzcompose.services.redpanda import REDPANDA_VERSION


class RedpandaDeployment(K8sDeployment):
    def __init__(self, namespace: str, apply_node_selectors: bool) -> None:
        super().__init__(namespace)
        container = V1Container(
            name="redpanda",
            image=f"redpandadata/redpanda:{REDPANDA_VERSION}",
            command=[
                "/usr/bin/rpk",
                "redpanda",
                "start",
                "--overprovisioned",
                "--smp",
                "1",
                "--memory",
                "1G",
                "--reserve-memory",
                "0M",
                "--node-id",
                "0",
                "--check=false",
                "--set",
                "redpanda.enable_transactions=true",
                "--set",
                "redpanda.enable_idempotence=true",
                "--set",
                "redpanda.auto_create_topics_enabled=true",
                # Only require 4KB per topic partition rather than 4MiB.
                "--set",
                "redpanda.topic_memory_per_partition=4096",
                "--advertise-kafka-addr",
                f"redpanda.{namespace}:9092",
            ],
        )

        node_selector = None
        if apply_node_selectors:
            node_selector = {"supporting-services": "true"}

        template = V1PodTemplateSpec(
            metadata=V1ObjectMeta(namespace=namespace, labels={"app": "redpanda"}),
            spec=V1PodSpec(containers=[container], node_selector=node_selector),
        )

        selector = V1LabelSelector(match_labels={"app": "redpanda"})

        spec = V1DeploymentSpec(replicas=1, template=template, selector=selector)

        self.deployment = V1Deployment(
            api_version="apps/v1",
            kind="Deployment",
            metadata=V1ObjectMeta(name="redpanda", namespace=namespace),
            spec=spec,
        )


class RedpandaService(K8sService):
    def __init__(
        self,
        namespace: str,
    ) -> None:
        super().__init__(namespace)
        ports = [
            V1ServicePort(name="kafka", port=9092),
            V1ServicePort(name="schema-registry", port=8081),
        ]

        self.service = V1Service(
            metadata=V1ObjectMeta(
                name="redpanda", namespace=namespace, labels={"app": "redpanda"}
            ),
            spec=V1ServiceSpec(
                type="NodePort", ports=ports, selector={"app": "redpanda"}
            ),
        )


def redpanda_resources(
    namespace: str = DEFAULT_K8S_NAMESPACE, apply_node_selectors: bool = False
) -> list[K8sResource]:
    return [
        RedpandaDeployment(namespace, apply_node_selectors),
        RedpandaService(namespace),
    ]
