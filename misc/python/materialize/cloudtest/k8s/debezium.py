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

from materialize.cloudtest import DEFAULT_K8S_NAMESPACE
from materialize.cloudtest.k8s.api.k8s_deployment import K8sDeployment
from materialize.cloudtest.k8s.api.k8s_resource import K8sResource
from materialize.cloudtest.k8s.api.k8s_service import K8sService


class DebeziumDeployment(K8sDeployment):
    def __init__(
        self, namespace: str, redpanda_namespace: str, apply_node_selectors: bool
    ) -> None:
        super().__init__(namespace)
        ports = [V1ContainerPort(container_port=8083, name="debezium")]

        env = [
            V1EnvVar(
                name="BOOTSTRAP_SERVERS", value=f"redpanda.{redpanda_namespace}:9092"
            ),
            V1EnvVar(name="CONFIG_STORAGE_TOPIC", value="connect_configs"),
            V1EnvVar(name="OFFSET_STORAGE_TOPIC", value="connect_offsets"),
            V1EnvVar(name="STATUS_STORAGE_TOPIC", value="connect_statuses"),
            # We don't support JSON, so ensure that connect uses AVRO to encode messages and CSR to
            # record the schema
            V1EnvVar(
                name="KEY_CONVERTER", value="io.confluent.connect.avro.AvroConverter"
            ),
            V1EnvVar(
                name="VALUE_CONVERTER", value="io.confluent.connect.avro.AvroConverter"
            ),
            V1EnvVar(
                name="CONNECT_KEY_CONVERTER_SCHEMA_REGISTRY_URL",
                value=f"http://redpanda.{redpanda_namespace}:8081",
            ),
            V1EnvVar(
                name="CONNECT_VALUE_CONVERTER_SCHEMA_REGISTRY_URL",
                value=f"http://redpanda.{redpanda_namespace}:8081",
            ),
            V1EnvVar(
                name="CONNECT_OFFSET_COMMIT_POLICY", value="AlwaysCommitOffsetPolicy"
            ),
            V1EnvVar(name="CONNECT_ERRORS_RETRY_TIMEOUT", value="60000"),
            V1EnvVar(name="CONNECT_ERRORS_RETRY_DELAY_MAX_MS", value="1000"),
        ]

        container = V1Container(
            name="debezium", image="debezium/connect:1.9.6.Final", env=env, ports=ports
        )

        node_selector = None
        if apply_node_selectors:
            node_selector = {"supporting-services": "true"}

        pod_spec = V1PodSpec(
            containers=[container],
            node_selector=node_selector,
        )

        template = V1PodTemplateSpec(
            metadata=V1ObjectMeta(namespace=namespace, labels={"app": "debezium"}),
            spec=pod_spec,
        )

        selector = V1LabelSelector(match_labels={"app": "debezium"})

        spec = V1DeploymentSpec(replicas=1, template=template, selector=selector)

        self.deployment = V1Deployment(
            api_version="apps/v1",
            kind="Deployment",
            metadata=V1ObjectMeta(name="debezium", namespace=namespace),
            spec=spec,
        )


class DebeziumService(K8sService):
    def __init__(
        self,
        namespace: str,
    ) -> None:
        super().__init__(namespace)
        ports = [
            V1ServicePort(name="debezium", port=8083),
        ]

        self.service = V1Service(
            metadata=V1ObjectMeta(
                name="debezium", namespace=namespace, labels={"app": "debezium"}
            ),
            spec=V1ServiceSpec(
                type="NodePort", ports=ports, selector={"app": "debezium"}
            ),
        )


def debezium_resources(
    debezium_namespace: str = DEFAULT_K8S_NAMESPACE,
    redpanda_namespace: str = DEFAULT_K8S_NAMESPACE,
    apply_node_selectors: bool = False,
) -> list[K8sResource]:
    return [
        DebeziumDeployment(
            debezium_namespace,
            redpanda_namespace=redpanda_namespace,
            apply_node_selectors=apply_node_selectors,
        ),
        DebeziumService(debezium_namespace),
    ]
