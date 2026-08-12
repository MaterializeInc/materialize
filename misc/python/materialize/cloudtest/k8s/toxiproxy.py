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
    V1LabelSelector,
    V1ObjectMeta,
    V1PodSpec,
    V1PodTemplateSpec,
    V1Service,
    V1ServicePort,
    V1ServiceSpec,
)

from materialize.cloudtest.k8s.api.k8s_deployment import K8sDeployment
from materialize.cloudtest.k8s.api.k8s_service import K8sService

TOXIPROXY_IMAGE = "jauderho/toxiproxy:v2.8.0"


class ToxiproxyDeployment(K8sDeployment):
    """Kubernetes Deployment for Toxiproxy.

    Args:
        namespace: Kubernetes namespace
        name: Name for this toxiproxy instance (e.g., "toxiproxy" or "toxiproxy-az1")
        apply_node_selectors: Whether to apply node selectors for supporting services
    """

    def __init__(
        self,
        namespace: str,
        name: str = "toxiproxy",
        apply_node_selectors: bool = False,
    ) -> None:
        super().__init__(namespace)
        self._name = name
        app_label = name

        container = V1Container(
            name="toxiproxy",
            image=TOXIPROXY_IMAGE,
            args=["-host=0.0.0.0"],
            ports=[
                V1ContainerPort(name="admin", container_port=8474),
                V1ContainerPort(name="kafka-proxy", container_port=9092),
            ],
        )

        node_selector = None
        if apply_node_selectors:
            node_selector = {"supporting-services": "true"}

        template = V1PodTemplateSpec(
            metadata=V1ObjectMeta(namespace=namespace, labels={"app": app_label}),
            spec=V1PodSpec(containers=[container], node_selector=node_selector),
        )

        selector = V1LabelSelector(match_labels={"app": app_label})

        spec = V1DeploymentSpec(replicas=1, template=template, selector=selector)

        self.deployment = V1Deployment(
            api_version="apps/v1",
            kind="Deployment",
            metadata=V1ObjectMeta(name=name, namespace=namespace),
            spec=spec,
        )

    def delete(self) -> None:
        self.apps_api().delete_namespaced_deployment(
            name=self._name, namespace=self.namespace()
        )


class ToxiproxyService(K8sService):
    """Kubernetes Service for Toxiproxy.

    Args:
        namespace: Kubernetes namespace
        name: Name for this toxiproxy service (should match deployment name)
    """

    def __init__(self, namespace: str, name: str = "toxiproxy") -> None:
        super().__init__(namespace)
        self._name = name
        app_label = name

        ports = [
            V1ServicePort(name="admin", port=8474),
            V1ServicePort(name="kafka-proxy", port=9092),
        ]

        self.service = V1Service(
            metadata=V1ObjectMeta(
                name=name, namespace=namespace, labels={"app": app_label}
            ),
            spec=V1ServiceSpec(
                type="NodePort", ports=ports, selector={"app": app_label}
            ),
        )

    def delete(self) -> None:
        self.api().delete_namespaced_service(
            name=self._name, namespace=self.namespace()
        )


class PrivateLinkExternalNameService(K8sService):
    """Creates an ExternalName service to simulate VpcEndpoint DNS resolution.

    In production, the environment-controller creates this service when a
    VpcEndpoint becomes "available". The service name follows the pattern:
    - 'connection-{catalog_id}' for the base endpoint
    - 'connection-{catalog_id}-{az}' for AZ-specific endpoints

    For testing, we point it to Toxiproxy which proxies to the actual service.

    Args:
        connection_id: The CatalogItemId of the PrivateLink connection (e.g., "u5")
        target_service: DNS name to resolve to (e.g., "toxiproxy.default.svc.cluster.local")
        namespace: Kubernetes namespace
        availability_zone: Optional AZ for AZ-specific endpoint (e.g., "use1-az1")
    """

    def __init__(
        self,
        connection_id: str,
        target_service: str,
        namespace: str,
        availability_zone: str | None = None,
    ) -> None:
        super().__init__(namespace)
        # Name matches vpc_endpoint_host() in src/cloud-resources/src/vpc_endpoint.rs
        if availability_zone:
            self._name = f"connection-{connection_id}-{availability_zone}"
        else:
            self._name = f"connection-{connection_id}"

        self.service = V1Service(
            api_version="v1",
            kind="Service",
            metadata=V1ObjectMeta(name=self._name, namespace=namespace),
            spec=V1ServiceSpec(
                type="ExternalName",
                external_name=target_service,
            ),
        )

    def delete(self) -> None:
        self.api().delete_namespaced_service(
            name=self._name, namespace=self.namespace()
        )
