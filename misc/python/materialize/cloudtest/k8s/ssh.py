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


class SshDeployment(K8sDeployment):
    def __init__(self) -> None:
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

        template = V1PodTemplateSpec(
            metadata=V1ObjectMeta(labels={"app": "ssh-bastion-host"}),
            spec=V1PodSpec(containers=[container]),
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
    def __init__(self) -> None:
        ports = [
            V1ServicePort(name="ssh", port=22),
        ]

        self.service = V1Service(
            metadata=V1ObjectMeta(
                name="ssh-bastion-host", labels={"app": "ssh-bastion-host"}
            ),
            spec=V1ServiceSpec(
                type="NodePort", ports=ports, selector={"app": "ssh-bastion-host"}
            ),
        )


SSH_RESOURCES = [SshDeployment(), SshService()]
