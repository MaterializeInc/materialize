# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


from kubernetes.client import V1ConfigMap
from kubernetes.client.exceptions import ApiException

from materialize.cloudtest.k8s.api.k8s_resource import K8sResource


class K8sConfigMap(K8sResource):
    config_map: V1ConfigMap

    def kind(self) -> str:
        return "configmap"

    def create(self) -> None:
        core_v1_api = self.api()

        # kubectl delete all -all does not clean up configmaps
        try:
            assert self.config_map.metadata is not None
            assert self.config_map.metadata.name is not None
            core_v1_api.delete_namespaced_config_map(
                name=self.config_map.metadata.name, namespace=self.namespace()
            )
        except ApiException:
            pass

        core_v1_api.create_namespaced_config_map(
            body=self.config_map, namespace=self.namespace()
        )
