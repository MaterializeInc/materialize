# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from kubernetes.client import V1ObjectMeta, V1Service, V1ServicePort, V1ServiceSpec

from materialize.cloudtest.k8s import K8sService


class PersistPubSubService(K8sService):
    def __init__(self) -> None:
        self.service = V1Service(
            api_version="v1",
            kind="Service",
            metadata=V1ObjectMeta(name="persist-pubsub"),
            spec=V1ServiceSpec(
                type="ClusterIP",
                cluster_ip=None,
                ports=[
                    V1ServicePort(
                        name="grpc", port=6879, target_port=6879, protocol="TCP"
                    )
                ],
                selector={"app": "environmentd"},
            ),
        )
