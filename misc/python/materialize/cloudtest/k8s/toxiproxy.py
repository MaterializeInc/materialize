# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import requests
from kubernetes.client import (
    V1Container,
    V1ContainerPort,
    V1Deployment,
    V1DeploymentSpec,
    V1HTTPGetAction,
    V1LabelSelector,
    V1ObjectMeta,
    V1PodSpec,
    V1PodTemplateSpec,
    V1Probe,
    V1Service,
    V1ServicePort,
    V1ServiceSpec,
)

from materialize.cloudtest.k8s.api.k8s_deployment import K8sDeployment
from materialize.cloudtest.k8s.api.k8s_service import K8sService
from materialize.cloudtest.util.common import retry
from materialize.mzcompose.services.redpanda import REDPANDA_VERSION

TOXIPROXY_IMAGE = "jauderho/toxiproxy:v2.8.0"

TOXIPROXY_ADMIN_PORT = 8474
TOXIPROXY_PROXY_PORT = 9092

ADMIN_REQUEST_TIMEOUT_SECS = 10


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
                V1ContainerPort(name="admin", container_port=TOXIPROXY_ADMIN_PORT),
                V1ContainerPort(
                    name="kafka-proxy", container_port=TOXIPROXY_PROXY_PORT
                ),
            ],
            readiness_probe=V1Probe(
                http_get=V1HTTPGetAction(path="/proxies", port="admin"),
                period_seconds=1,
                failure_threshold=30,
            ),
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
            V1ServicePort(name="admin", port=TOXIPROXY_ADMIN_PORT),
            V1ServicePort(name="kafka-proxy", port=TOXIPROXY_PROXY_PORT),
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

    def _admin_url(self, path: str) -> str:
        return f"http://localhost:{self.node_port('admin')}/{path.lstrip('/')}"

    def wait_for_admin_ready(self, max_attempts: int = 60) -> None:
        """Block until the admin API answers through the NodePort.

        Must be called before any other admin request. The pod's readiness
        probe only gates the pod, not the node-local routing to it: kube-proxy
        programs the NodePort rules asynchronously, so the first connection can
        still be reset. Retrying an idempotent GET until it lands proves the
        path, which is what lets the mutating helpers issue their request
        exactly once. Retrying those instead would not be safe, since a
        duplicate proxy creation is rejected with 409.
        """

        def get_proxies() -> None:
            response = requests.get(
                self._admin_url("/proxies"), timeout=ADMIN_REQUEST_TIMEOUT_SECS
            )
            response.raise_for_status()

        retry(
            f=get_proxies,
            max_attempts=max_attempts,
            exception_types=[requests.exceptions.RequestException],
            message=f"toxiproxy {self._name} admin API never became reachable",
        )

    def create_proxy(self, name: str, upstream: str, enabled: bool = True) -> None:
        """Create a proxy listening on the deployment's proxy port."""
        self._post_proxy("/proxies", name, upstream, enabled)

    def set_proxy_enabled(self, name: str, upstream: str, enabled: bool) -> None:
        """Enable or disable an existing proxy.

        Disabling closes all open connections and refuses new ones, which is
        how these tests simulate a PrivateLink endpoint going away.
        """
        self._post_proxy(f"/proxies/{name}", name, upstream, enabled)

    def _post_proxy(self, path: str, name: str, upstream: str, enabled: bool) -> None:
        response = requests.post(
            self._admin_url(path),
            json={
                "name": name,
                "listen": f"0.0.0.0:{TOXIPROXY_PROXY_PORT}",
                "upstream": upstream,
                "enabled": enabled,
            },
            timeout=ADMIN_REQUEST_TIMEOUT_SECS,
        )
        response.raise_for_status()


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


class PrivateLinkTestRedpandaDeployment(K8sDeployment):
    """Redpanda deployment that advertises an AZ-specific hostname for PrivateLink testing.

    This allows testing pattern-based broker routing where the advertised broker
    address contains an AZ identifier that can be matched by routing rules.

    Args:
        namespace: Kubernetes namespace
        name: Name for this Redpanda instance
        advertise_addr: The address Redpanda advertises to clients (e.g., "broker.use1-az1.internal:9092")
        apply_node_selectors: Whether to apply node selectors
    """

    def __init__(
        self,
        namespace: str,
        name: str = "redpanda-privatelink",
        advertise_addr: str = "broker.use1-az1.internal:9092",
        apply_node_selectors: bool = False,
    ) -> None:
        super().__init__(namespace)
        self._name = name
        app_label = name

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
                "--set",
                "redpanda.topic_memory_per_partition=4096",
                "--advertise-kafka-addr",
                advertise_addr,
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


class PrivateLinkTestRedpandaService(K8sService):
    """Service for the PrivateLink test Redpanda instance.

    Args:
        namespace: Kubernetes namespace
        name: Name for this service (should match deployment name)
    """

    def __init__(self, namespace: str, name: str = "redpanda-privatelink") -> None:
        super().__init__(namespace)
        self._name = name
        app_label = name

        ports = [
            V1ServicePort(name="kafka", port=9092),
            V1ServicePort(name="schema-registry", port=8081),
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
