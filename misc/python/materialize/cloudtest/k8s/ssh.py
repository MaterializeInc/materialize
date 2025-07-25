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


class SshDeployment(K8sDeployment):
    def __init__(self, namespace: str, apply_node_selectors: bool) -> None:
        super().__init__(namespace)
        env = [
            V1EnvVar(name="SSH_USERS", value="mz:1000:1000"),
            V1EnvVar(name="TCP_FORWARDING", value="true"),
        ]
        ports = [V1ContainerPort(container_port=22, name="ssh")]
        container = V1Container(
            name="ssh-bastion-host",
            image="panubo/sshd:1.5.0",
            env=env,
            ports=ports,
        )

        node_selector = None
        if apply_node_selectors:
            node_selector = {"supporting-services": "true"}

        template = V1PodTemplateSpec(
            metadata=V1ObjectMeta(labels={"app": "ssh-bastion-host"}),
            spec=V1PodSpec(containers=[container], node_selector=node_selector),
        )

        selector = V1LabelSelector(match_labels={"app": "ssh-bastion-host"})

        spec = V1DeploymentSpec(replicas=1, template=template, selector=selector)

        self.deployment = V1Deployment(
            api_version="apps/v1",
            kind="Deployment",
            metadata=V1ObjectMeta(name="ssh-bastion-host"),
            spec=spec,
        )


class SshService(K8sService):
    def __init__(self, namespace: str) -> None:
        super().__init__(namespace)
        ports = [
            V1ServicePort(name="ssh", port=23, target_port=22),
        ]

        self.service = V1Service(
            metadata=V1ObjectMeta(
                name="ssh-bastion-host", labels={"app": "ssh-bastion-host"}
            ),
            spec=V1ServiceSpec(
                type="NodePort", ports=ports, selector={"app": "ssh-bastion-host"}
            ),
        )


def ssh_resources(
    namespace: str = DEFAULT_K8S_NAMESPACE, apply_node_selectors: bool = False
) -> list[K8sResource]:
    return [SshDeployment(namespace, apply_node_selectors), SshService(namespace)]
