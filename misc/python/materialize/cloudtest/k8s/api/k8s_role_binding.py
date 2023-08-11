# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


from kubernetes.client import V1RoleBinding
from kubernetes.client.exceptions import ApiException

from materialize.cloudtest import DEFAULT_K8S_NAMESPACE
from materialize.cloudtest.k8s.api.k8s_resource import K8sResource


class K8sRoleBinding(K8sResource):
    role_binding: V1RoleBinding

    def __init__(self, namespace: str = DEFAULT_K8S_NAMESPACE):
        super().__init__(namespace)

    def kind(self) -> str:
        return "rolebinding"

    def create(self) -> None:
        rbac_api = self.rbac_api()

        # kubectl delete all -all does not clean up role bindings
        try:
            assert self.role_binding.metadata is not None
            assert self.role_binding.metadata.name is not None
            rbac_api.delete_namespaced_role_binding(
                name=self.role_binding.metadata.name, namespace=self.namespace()
            )
        except ApiException:
            pass

        rbac_api.create_namespaced_role_binding(
            body=self.role_binding,
            namespace=self.namespace(),
        )
