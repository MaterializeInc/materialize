# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


from kubernetes.client import V1Deployment

from materialize.cloudtest.k8s.api.k8s_resource import K8sResource


class K8sDeployment(K8sResource):
    deployment: V1Deployment

    def kind(self) -> str:
        return "deployment"

    def create(self) -> None:
        apps_v1_api = self.apps_api()
        apps_v1_api.create_namespaced_deployment(
            body=self.deployment, namespace=self.namespace()
        )
