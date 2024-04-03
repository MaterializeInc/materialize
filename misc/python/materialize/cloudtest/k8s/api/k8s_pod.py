# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


from kubernetes.client import V1Pod

from materialize.cloudtest import DEFAULT_K8S_NAMESPACE
from materialize.cloudtest.k8s.api.k8s_resource import K8sResource


class K8sPod(K8sResource):
    pod: V1Pod

    def __init__(self, namespace: str = DEFAULT_K8S_NAMESPACE):
        super().__init__(namespace)

    def kind(self) -> str:
        return "pod"

    def create(self) -> None:
        core_v1_api = self.api()
        core_v1_api.create_namespaced_pod(body=self.pod, namespace=self.namespace())

    def name(self) -> str:
        assert self.pod.metadata is not None
        assert self.pod.metadata.name is not None
        return self.pod.metadata.name

    def copy(self, source: str, destination: str) -> None:
        self.kubectl("cp", source, f"{self.name()}:{destination}")

    def delete(self, path: str) -> None:
        self.kubectl("exec", self.name(), "--", "rm", path)
