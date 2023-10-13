# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from kubernetes.client import V1ClusterRole
from kubernetes.client.exceptions import ApiException

from materialize.cloudtest import DEFAULT_K8S_NAMESPACE
from materialize.cloudtest.k8s.api.k8s_resource import K8sResource


class K8sClusterRole(K8sResource):
    role: V1ClusterRole

    def __init__(self, namespace: str = DEFAULT_K8S_NAMESPACE):
        super().__init__(namespace)

    def kind(self) -> str:
        return "clusterrole"

    def create(self) -> None:
        rbac_api = self.rbac_api()

        # kubectl delete all -all does not clean up role bindings
        try:
            assert self.role.metadata is not None
            assert self.role.metadata.name is not None
            rbac_api.delete_cluster_role(name=self.role.metadata.name)
        except ApiException:
            pass

        rbac_api.create_cluster_role(
            body=self.role,
        )
