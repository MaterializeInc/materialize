# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""The mz-debug collector, deployed directly rather than by the operator.

cloudtest runs environmentd without the Materialize operator, so there is no
MaterializeDebug resource to reconcile a collector from. These resources stand
in for what the operator's debug controller would create: a Deployment running
`mz-debug collector` against the instance and a Service exposing its HTTP API.
The collector runs as the namespace's default service account, which
cloudtest grants the `admin` ClusterRole, so it can read the namespace but
not cluster-scoped kinds; those are logged and skipped.
"""

from kubernetes.client import (
    V1Container,
    V1ContainerPort,
    V1Deployment,
    V1DeploymentSpec,
    V1EmptyDirVolumeSource,
    V1LabelSelector,
    V1ObjectMeta,
    V1PodSpec,
    V1PodTemplateSpec,
    V1Service,
    V1ServicePort,
    V1ServiceSpec,
    V1Volume,
    V1VolumeMount,
)

from materialize.cloudtest import DEFAULT_K8S_NAMESPACE
from materialize.cloudtest.k8s.api.k8s_deployment import K8sDeployment
from materialize.cloudtest.k8s.api.k8s_service import K8sService
from materialize.cloudtest.k8s.environmentd import MzInstanceIdentity

COLLECTOR_HTTP_PORT = 8080
COLLECTOR_LABELS = {"app": "debug-collector"}


class DebugCollectorDeployment(K8sDeployment):
    def __init__(
        self,
        instance_identity: MzInstanceIdentity,
        tag: str | None = None,
        release_mode: bool = True,
        namespace: str = DEFAULT_K8S_NAMESPACE,
    ) -> None:
        super().__init__(namespace)
        container = V1Container(
            name="debug-collector",
            image=self.image("mz-debug", tag=tag, release_mode=release_mode),
            args=[
                "collector",
                f"--k8s-namespace={namespace}",
                f"--mz-instance-name={instance_identity.organization_name}",
                f"--listen-addr=0.0.0.0:{COLLECTOR_HTTP_PORT}",
                "--auth-mode=none",
                # Tests request the snapshots they need through the API. Keep
                # the periodic snapshots empty so the collector does not put
                # load on the instance while unrelated tests run.
                "--snapshot-interval=24h",
                "--dump-k8s=false",
                "--dump-system-catalog=false",
                "--dump-heap-profiles=false",
                "--dump-prometheus-metrics=false",
            ],
            ports=[V1ContainerPort(container_port=COLLECTOR_HTTP_PORT, name="http")],
            volume_mounts=[
                V1VolumeMount(name="snapshots", mount_path="/var/lib/mz-debug")
            ],
        )
        metadata = V1ObjectMeta(name="debug-collector", labels=COLLECTOR_LABELS)
        self.deployment = V1Deployment(
            api_version="apps/v1",
            kind="Deployment",
            metadata=metadata,
            spec=V1DeploymentSpec(
                replicas=1,
                selector=V1LabelSelector(match_labels=COLLECTOR_LABELS),
                template=V1PodTemplateSpec(
                    metadata=metadata,
                    spec=V1PodSpec(
                        containers=[container],
                        volumes=[
                            V1Volume(
                                name="snapshots", empty_dir=V1EmptyDirVolumeSource()
                            )
                        ],
                    ),
                ),
            ),
        )


class DebugCollectorService(K8sService):
    def __init__(self, namespace: str = DEFAULT_K8S_NAMESPACE) -> None:
        super().__init__(namespace)
        self.service = V1Service(
            api_version="v1",
            kind="Service",
            metadata=V1ObjectMeta(name="debug-collector", labels=COLLECTOR_LABELS),
            spec=V1ServiceSpec(
                type="NodePort",
                ports=[V1ServicePort(name="http", port=COLLECTOR_HTTP_PORT)],
                selector=COLLECTOR_LABELS,
            ),
        )

    def base_url(self) -> str:
        """The collector's HTTP API as reachable from the test host."""
        return f"http://localhost:{self.node_port('http')}"
