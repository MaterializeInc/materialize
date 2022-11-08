# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import subprocess
from typing import Any, Optional

import pg8000
import sqlparse
from kubernetes.client import (
    AppsV1Api,
    CoreV1Api,
    RbacAuthorizationV1Api,
    V1ClusterRole,
    V1ConfigMap,
    V1Deployment,
    V1Pod,
    V1RoleBinding,
    V1Secret,
    V1Service,
    V1StatefulSet,
)
from kubernetes.client.exceptions import ApiException
from kubernetes.config import new_client_from_config  # type: ignore
from pg8000 import Connection, Cursor

from materialize import ROOT, mzbuild


class K8sResource:
    def kubectl(self, *args: str) -> str:
        return subprocess.check_output(
            ["kubectl", "--context", self.context(), *args]
        ).decode("ascii")

    def api(self) -> CoreV1Api:
        api_client = new_client_from_config(context=self.context())
        return CoreV1Api(api_client)

    def apps_api(self) -> AppsV1Api:
        api_client = new_client_from_config(context=self.context())
        return AppsV1Api(api_client)

    def rbac_api(self) -> RbacAuthorizationV1Api:
        api_client = new_client_from_config(context=self.context())
        return RbacAuthorizationV1Api(api_client)

    def context(self) -> str:
        return "kind-kind"

    def namespace(self) -> str:
        return "default"

    def kind(self) -> str:
        assert False

    def create(self) -> None:
        assert False

    def image(
        self, service: str, tag: Optional[str] = None, release_mode: bool = True
    ) -> str:
        if tag is not None:
            return f"materialize/{service}:{tag}"
        else:
            repo = mzbuild.Repository(ROOT, release_mode=release_mode)
            deps = repo.resolve_dependencies([repo.images[service]])
            rimage = deps[service]
            return rimage.spec()


class K8sPod(K8sResource):
    pod: V1Pod

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


class K8sService(K8sResource):
    service: V1Service

    def kind(self) -> str:
        return "service"

    def create(self) -> None:
        core_v1_api = self.api()
        core_v1_api.create_namespaced_service(
            body=self.service, namespace=self.namespace()
        )

    def node_port(self, name: Optional[str] = None) -> int:
        assert self.service and self.service.metadata and self.service.metadata.name
        service = self.api().read_namespaced_service(
            self.service.metadata.name, self.namespace()
        )
        assert service is not None

        spec = service.spec
        assert spec is not None

        ports = spec.ports
        assert ports is not None and len(ports) > 0

        port = next(p for p in ports if name is None or p.name == name)

        node_port = port.node_port
        assert node_port is not None

        return node_port

    def sql_conn(
        self,
        port: Optional[str] = None,
        user: str = "materialize",
    ) -> Connection:
        """Get a connection to run SQL queries against the service"""
        return pg8000.connect(
            host="localhost",
            port=self.node_port(name=port),
            user=user,
        )

    def sql_cursor(
        self,
        port: Optional[str] = None,
        user: str = "materialize",
    ) -> Cursor:
        """Get a cursor to run SQL queries against the service"""
        conn = self.sql_conn(port=port, user=user)
        conn.autocommit = True
        return conn.cursor()

    def sql(
        self,
        sql: str,
        port: Optional[str] = None,
        user: str = "materialize",
    ) -> None:
        """Run a batch of SQL statements against the service."""
        with self.sql_cursor(port=port, user=user) as cursor:
            for statement in sqlparse.split(sql):
                print(f"> {statement}")
                cursor.execute(statement)

    def sql_query(
        self,
        sql: str,
        port: Optional[str] = None,
        user: str = "materialize",
    ) -> Any:
        """Execute a SQL query against the service and return results."""
        with self.sql_cursor(port=port, user=user) as cursor:
            print(f"> {sql}")
            cursor.execute(sql)
            return cursor.fetchall()


class K8sDeployment(K8sResource):
    deployment: V1Deployment

    def kind(self) -> str:
        return "deployment"

    def create(self) -> None:
        apps_v1_api = self.apps_api()
        apps_v1_api.create_namespaced_deployment(
            body=self.deployment, namespace=self.namespace()
        )


class K8sStatefulSet(K8sResource):
    stateful_set: V1StatefulSet

    def __init__(self) -> None:
        self.stateful_set = self.generate_stateful_set()

    def generate_stateful_set(self) -> V1StatefulSet:
        assert False

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
        print(f"Replacing stateful set {name}...")
        self.stateful_set = self.generate_stateful_set()
        apps_v1_api.replace_namespaced_stateful_set(
            name=name, body=self.stateful_set, namespace=self.namespace()
        )


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


class K8sClusterRole(K8sResource):
    role: V1ClusterRole

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


class K8sRoleBinding(K8sResource):
    role_binding: V1RoleBinding

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


class K8sSecret(K8sResource):
    secret = V1Secret

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
