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
    V1VolumeMount,
)

from materialize.cloudtest import DEFAULT_K8S_NAMESPACE
from materialize.cloudtest.k8s.api.k8s_deployment import K8sDeployment
from materialize.cloudtest.k8s.api.k8s_resource import K8sResource
from materialize.cloudtest.k8s.api.k8s_service import K8sService


class FivetranDestination(K8sService):
    def __init__(
        self,
        namespace: str,
    ) -> None:
        super().__init__(namespace)
        service_port = V1ServicePort(name="fivetran-destination", port=6874)

        self.service = V1Service(
            api_version="v1",
            kind="Service",
            metadata=V1ObjectMeta(
                name="fivetran-destination",
                namespace=namespace,
                labels={"app": "fivetran-destination"},
            ),
            spec=V1ServiceSpec(
                type="NodePort",
                ports=[service_port],
                selector={"app": "fivetran-destination"},
            ),
        )


class FivetranDestinationDeployment(K8sDeployment):
    def __init__(self, namespace: str, apply_node_selectors: bool) -> None:
        super().__init__(namespace)

        volume_mounts = [
            V1VolumeMount(name="data", mount_path="/data"),
        ]

        container = V1Container(
            name="fivetran-destination",
            image=self.image(
                "fivetran-destination", tag="mzbuild-LFLUC5COYYJAXIEEG7QIV2OFTVTUH5Y4"
            ),
            volume_mounts=volume_mounts,
        )

        node_selector = None
        if apply_node_selectors:
            node_selector = {"supporting-services": "true"}

        template = V1PodTemplateSpec(
            metadata=V1ObjectMeta(
                namespace=namespace, labels={"app": "fivetran-destination"}
            ),
            spec=V1PodSpec(containers=[container], node_selector=node_selector),
        )

        selector = V1LabelSelector(match_labels={"app": "fivetran-destination"})

        spec = V1DeploymentSpec(template=template, selector=selector)

        self.deployment = V1Deployment(
            api_version="apps/v1",
            kind="Deployment",
            metadata=V1ObjectMeta(name="fivetran-destination", namespace=namespace),
            spec=spec,
        )


# TODO: probably not needed
# class FivetranDestinationTesterDeployment(K8sDeployment):
#     def __init__(self, namespace: str, apply_node_selectors: bool) -> None:
#         super().__init__(namespace)
#
#         volume_mounts = [
#             V1VolumeMount(name="data", mount_path="/data"),
#         ]
#
#         env = [
#             V1EnvVar(name="GRPC_HOSTNAME", value="fivetran-destination"),
#         ]
#
#         command = ["--port=6874"]
#
#         container = V1Container(
#             name="fivetran-destination-tester",
#             image=self.image(
#                 "fivetran-destination-tester",
#                 tag="mzbuild-VW54BM6M7R2WD6UXNFDMEJJU7SD4TUBI",
#             ),
#             env=env,
#             command=command,
#             volume_mounts=volume_mounts,
#         )
#
#         node_selector = None
#         if apply_node_selectors:
#             node_selector = {"supporting-services": "true"}
#
#         template = V1PodTemplateSpec(
#             metadata=V1ObjectMeta(
#                 namespace=namespace, labels={"app": "fivetran-destination-tester"}
#             ),
#             spec=V1PodSpec(containers=[container], node_selector=node_selector),
#         )
#
#         selector = V1LabelSelector(match_labels={"app": "fivetran-destination-tester"})
#
#         spec = V1DeploymentSpec(template=template, selector=selector)
#
#         self.deployment = V1Deployment(
#             api_version="apps/v1",
#             kind="Deployment",
#             metadata=V1ObjectMeta(
#                 name="fivetran-destination-tester", namespace=namespace
#             ),
#             spec=spec,
#         )


def fivetran_resources(
    namespace: str = DEFAULT_K8S_NAMESPACE, apply_node_selectors: bool = False
) -> list[K8sResource]:
    return [
        FivetranDestination(namespace),
        FivetranDestinationDeployment(namespace, apply_node_selectors),
        # TODO: probably not needed for td tests
        # FivetranDestinationTesterDeployment(namespace, apply_node_selectors),
    ]
