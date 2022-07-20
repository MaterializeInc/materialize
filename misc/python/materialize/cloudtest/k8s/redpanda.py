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

from materialize.cloudtest.k8s import K8sDeployment, K8sService


class RedpandaDeployment(K8sDeployment):
    def __init__(self) -> None:
        container = V1Container(
            name="redpanda",
            image="vectorized/redpanda:v21.11.13",
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
                "redpanda.auto_create_topics_enabled=false",
                "--advertise-kafka-addr",
                "redpanda:9092",
            ],
        )

        template = V1PodTemplateSpec(
            metadata=V1ObjectMeta(labels={"app": "redpanda"}),
            spec=V1PodSpec(containers=[container]),
        )

        selector = V1LabelSelector(match_labels={"app": "redpanda"})

        spec = V1DeploymentSpec(replicas=1, template=template, selector=selector)

        self.deployment = V1Deployment(
            api_version="apps/v1",
            kind="Deployment",
            metadata=V1ObjectMeta(name="redpanda"),
            spec=spec,
        )


class RedpandaService(K8sService):
    def __init__(self) -> None:
        ports = [
            V1ServicePort(name="kafka", port=9092),
            V1ServicePort(name="schema-registry", port=8081),
        ]

        self.service = V1Service(
            metadata=V1ObjectMeta(name="redpanda", labels={"app": "redpanda"}),
            spec=V1ServiceSpec(
                type="NodePort", ports=ports, selector={"app": "redpanda"}
            ),
        )


REDPANDA_RESOURCES = [RedpandaDeployment(), RedpandaService()]
