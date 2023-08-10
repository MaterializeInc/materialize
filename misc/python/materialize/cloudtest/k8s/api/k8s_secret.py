# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


from kubernetes.client import V1Secret
from kubernetes.client.exceptions import ApiException

from materialize.cloudtest import DEFAULT_K8S_NAMESPACE
from materialize.cloudtest.k8s.api.k8s_resource import K8sResource


class K8sSecret(K8sResource):
    secret = V1Secret

    def __init__(self, namespace: str = DEFAULT_K8S_NAMESPACE):
        super().__init__(namespace)

    def kind(self) -> str:
        return "secret"

    # kubectl delete all -all does not clean up secrets
    def create(self) -> None:
        core_v1_api = self.api()

        try:
            assert self.secret.metadata is not None
            assert self.secret.metadata.name is not None
            core_v1_api.delete_namespaced_secret(
                name=self.secret.metadata.name, namespace=self.namespace()
            )
        except ApiException:
            pass
        core_v1_api.create_namespaced_secret(
            body=self.secret, namespace=self.namespace()  # type: ignore
        )
