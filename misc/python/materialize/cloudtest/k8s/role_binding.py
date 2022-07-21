# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from kubernetes.client import V1ObjectMeta, V1RoleBinding, V1RoleRef, V1Subject

from materialize.cloudtest.k8s import K8sRoleBinding


class AdminRoleBinding(K8sRoleBinding):
    def __init__(self) -> None:
        metadata = V1ObjectMeta(name="admin-binding")
        role_ref = V1RoleRef(
            api_group="rbac.authorization.k8s.io", kind="ClusterRole", name="admin"
        )
        subjects = [V1Subject(kind="ServiceAccount", name="default")]
        self.role_binding = V1RoleBinding(
            api_version="rbac.authorization.k8s.io/v1",
            kind="RoleBinding",
            metadata=metadata,
            role_ref=role_ref,
            subjects=subjects,
        )
