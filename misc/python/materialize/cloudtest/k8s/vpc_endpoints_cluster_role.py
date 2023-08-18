# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from kubernetes.client import V1ClusterRole, V1ObjectMeta, V1PolicyRule

from materialize.cloudtest import DEFAULT_K8S_NAMESPACE
from materialize.cloudtest.k8s.api.k8s_cluster_role import K8sClusterRole


class VpcEndpointsClusterRole(K8sClusterRole):
    def __init__(self, namespace: str = DEFAULT_K8S_NAMESPACE) -> None:
        super().__init__(namespace)
        metadata = V1ObjectMeta(
            name="vpcendpoints",
            labels={"rbac.authorization.k8s.io/aggregate-to-admin": "true"},
        )
        self.role = V1ClusterRole(
            api_version="rbac.authorization.k8s.io/v1",
            kind="ClusterRole",
            metadata=metadata,
            rules=[
                V1PolicyRule(
                    api_groups=["materialize.cloud"],
                    resources=["vpcendpoints"],
                    verbs=[
                        "get",
                        "list",
                        "watch",
                        "create",
                        "update",
                        "patch",
                        "delete",
                    ],
                ),
            ],
        )
