# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from kubernetes.client import V1ObjectMeta, V1Service, V1ServicePort, V1ServiceSpec

from materialize.cloudtest import DEFAULT_K8S_NAMESPACE
from materialize.cloudtest.k8s.api.k8s_service import K8sService


class PersistCommitterService(K8sService):
    # Exposes the in-`environmentd` persist committer so that clusterds, which
    # run in their own pods, can reach it. Mirrors `PersistPubSubService`;
    # without it clusterds fall back to a loopback committer URL that resolves
    # to their own (empty) pod.
    def __init__(self, namespace: str = DEFAULT_K8S_NAMESPACE) -> None:
        super().__init__(namespace)
        self.service = V1Service(
            api_version="v1",
            kind="Service",
            metadata=V1ObjectMeta(name="persist-committer"),
            spec=V1ServiceSpec(
                type="ClusterIP",
                cluster_ip=None,
                ports=[
                    V1ServicePort(
                        name="grpc", port=6882, target_port=6882, protocol="TCP"
                    )
                ],
                selector={"app": "environmentd"},
            ),
        )
