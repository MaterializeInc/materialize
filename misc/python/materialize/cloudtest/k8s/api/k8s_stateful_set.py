# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


import logging

from kubernetes.client import V1StatefulSet

from materialize.cloudtest import DEFAULT_K8S_NAMESPACE
from materialize.cloudtest.k8s.api.k8s_resource import K8sResource

LOGGER = logging.getLogger(__name__)


class K8sStatefulSet(K8sResource):
    stateful_set: V1StatefulSet

    def __init__(self, namespace: str = DEFAULT_K8S_NAMESPACE):
        super().__init__(namespace)
        self.stateful_set = self.generate_stateful_set()

    def generate_stateful_set(self) -> V1StatefulSet:
        raise NotImplementedError

    def kind(self) -> str:
        return "statefulset"

    def name(self) -> str:
        assert self.stateful_set.metadata is not None
        assert self.stateful_set.metadata.name is not None
        return self.stateful_set.metadata.name

    def create(self) -> None:
        apps_v1_api = self.apps_api()
        apps_v1_api.create_namespaced_stateful_set(
            body=self.stateful_set, namespace=self.namespace()
        )

    def replace(self) -> None:
        apps_v1_api = self.apps_api()
        name = self.name()
        LOGGER.info(f"Replacing stateful set {name}...")
        self.stateful_set = self.generate_stateful_set()
        apps_v1_api.replace_namespaced_stateful_set(
            name=name, body=self.stateful_set, namespace=self.namespace()
        )
        # Despite the name "status" this kubectl command will actually wait
        # until the rollout is complete.
        # See https://github.com/kubernetes/kubernetes/issues/79606#issuecomment-779779928
        self.kubectl("rollout", "status", f"statefulset/{name}")
