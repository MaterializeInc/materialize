# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import logging
import os
import subprocess
import time
from datetime import datetime, timedelta
from typing import Any

import pg8000
import requests
import sqlparse
from kubernetes.client import V1Service, CoreV1Api
from kubernetes.config import new_client_from_config  # type: ignore
from pg8000 import Connection, Cursor
from pg8000.exceptions import InterfaceError

from materialize import MZ_ROOT, mzbuild, ui
from materialize.cli import orchestratord
from materialize.cloudtest import (
    DEFAULT_K8S_CLUSTER_NAME,
    DEFAULT_K8S_CONTEXT_NAME,
    DEFAULT_K8S_NAMESPACE,
)
from materialize.cloudtest.k8s.api.k8s_resource import K8sResource
from materialize.cloudtest.k8s.cockroach import cockroach_resources
from materialize.cloudtest.k8s.debezium import debezium_resources
from materialize.cloudtest.k8s.environmentd import (
    K8sMaterialize,
    BackendSecret,
    EnvironmentdService,
    #EnvironmentdStatefulSet,
    #ListenersConfigMap,
    #MaterializedAliasService,
)
from materialize.cloudtest.k8s.minio import Minio
from materialize.cloudtest.k8s.mysql import mysql_resources
from materialize.cloudtest.k8s.postgres import postgres_resources
from materialize.cloudtest.k8s.redpanda import redpanda_resources
from materialize.cloudtest.k8s.ssh import ssh_resources
from materialize.cloudtest.k8s.testdrive import TestdrivePod
from materialize.cloudtest.k8s.vpc_endpoints_cluster_role import VpcEndpointsClusterRole
from materialize.cloudtest.util.common import log_subprocess_error
from materialize.cloudtest.util.wait import wait

LOGGER = logging.getLogger(__name__)


#class Metadata(TypedDict):
#    name: str
#    namespace: str | None
#
#
#class ServicePort(TypedDict):
#    appProtocol: str | None
#    name: str | None
#    nodePort: str | None
#    port: int
#    protocol: str | None
#    targetPort: str | None
#
#
#class ServiceSpec(TypedDict):
#    ports: list[ServicePort]
#
#
#class Service(TypedDict):
#    metadata: Metadata
#    spec: ServiceSpec
#    status: ServiceStatus


#class MzService:
#    service: V1Service
#    
#    def __init__(self, service: V1Service):
#        self.service = service
#
#    def node_port(self, name: str | None = None) -> int:
#        assert self.service is not None
#        assert self.service.spec is not None
#
#        ports = self.service.spec.ports
#        assert ports is not None and len(ports) > 0
#
#        port = next(p for p in ports if name is None or p.name == name)
#
#        node_port = port.node_port
#        assert node_port is not None
#
#        return node_port
#
#    def sql_conn(
#        self,
#        port: str | None = None,
#        user: str = "materialize",
#    ) -> Connection:
#        """Get a connection to run SQL queries against the service"""
#        return pg8000.connect(
#            host="localhost",
#            port=self.node_port(name=port),
#            user=user,
#        )
#
#    def sql_cursor(
#        self,
#        port: str | None = None,
#        user: str = "materialize",
#        autocommit: bool = True,
#    ) -> Cursor:
#        """Get a cursor to run SQL queries against the service"""
#        conn = self.sql_conn(port=port, user=user)
#        conn.autocommit = autocommit
#        return conn.cursor()
#
#    def sql(
#        self,
#        sql: str,
#        port: str | None = None,
#        user: str = "materialize",
#    ) -> None:
#        """Run a batch of SQL statements against the service."""
#        with self.sql_cursor(port=port, user=user) as cursor:
#            for statement in sqlparse.split(sql):
#                LOGGER.info(f"> {statement}")
#                cursor.execute(statement)
#
#    def sql_query(
#        self,
#        sql: str,
#        port: str | None = None,
#        user: str = "materialize",
#    ) -> Any:
#        """Execute a SQL query against the service and return results."""
#        with self.sql_cursor(port=port, user=user) as cursor:
#            LOGGER.info(f"> {sql}")
#            cursor.execute(sql)
#            return cursor.fetchall()
#
#    def http_get(self, path: str) -> Any:
#        url = f"http://localhost:{self.node_port('internalhttp')}/{path}"
#        response = requests.get(url)
#        response.raise_for_status()
#        return response.text


class MaterializeApplication(object):
    def __init__(
        self,
        release_mode: bool,
        tag: str | None = None,
        aws_region: str | None = None,
        log_filter: str | None = None,
        apply_node_selectors: bool = False,
    ) -> None:
        self.tag = tag
        self.backend_secret = BackendSecret()
        self.materialize = K8sMaterialize(tag=self.tag)
        #self.materialize = K8sMaterialize(
        #    name="cloudtest",
        #    namespace="default",
        #    environmentd_image_ref=f"materialize/environmentd:{self.tag}",
        #    environmentd_extra_args=None,
        #    environmentd_extra_env=None,
        #    environment_id=None,
        #    authenticator_kind="None",
        #    enable_rbac=False,
        #)

        self.environmentd = EnvironmentdService()
        #self.materialized_alias = MaterializedAliasService()
        self.apply_node_selectors = apply_node_selectors
        self.release_mode = release_mode
        self.aws_region = aws_region
        self.mz_root = MZ_ROOT

        self.resources = self.get_resources(log_filter)
        self.images = self.get_images()

        # Register the VpcEndpoint CRD.
        self.register_vpc_endpoint()

        self.start_metrics_server()

        self.acquire_images()
        self.create_namespace()
        # TODO wait for minio and postgres to be configured
        self.install_orchestratord(values_file=str(MZ_ROOT / "test/cloudtest/orchestratord_values.yaml"))
        self.create_resources()
        self.wait_resource_creation_completed()
        self.resource_id = self.kubectl(
            "get",
            "materialize",
            self.materialize.name,
            "-o=jsonpath={.status.resourceId}",
            namespace=DEFAULT_K8S_NAMESPACE,
        )
        #api_client = new_client_from_config(context=self.context())
        #v1_api = CoreV1Api(api_client)
        #self.environmentd = MzService(service=v1_api.read_namespaced_service(self.prefixed("environmentd"), DEFAULT_K8S_NAMESPACE))
        # TODO get materialize object
        self.testdrive = TestdrivePod(
            release_mode=release_mode,
            aws_region=aws_region,
            apply_node_selectors=apply_node_selectors,
            materialize_url=f"postgres://materialize:materialize@{self.prefixed('environmentd')}:6875/materialize",
            materialize_internal_url=f"postgres://mz_system@{self.prefixed('environmentd')}:6877/materialize",
        )
        self.testdrive.create()
        wait(
            condition="condition=Ready",
            resource="pod/testdrive",
        )
        #self.run_materialize()

    def prefixed(self, name: str) -> str:
        return f"mz{self.resource_id}-{name}"

    def cluster_pod_name(self, cluster_id: str, replica_id: str, process: int = 0) -> str:
        pod_name = f"cluster-{cluster_id}-replica-{replica_id}-gen-0-{process}"
        return f"pod/{self.prefixed(pod_name)}"

    def cluster_service_name(self, cluster_id: str, replica_id: str) -> str:
        service_name = f"cluster-{cluster_id}-replica-{replica_id}-gen-0"
        return f"service/{self.prefixed(service_name)}"

    def get_resources(self, log_filter: str | None) -> list[K8sResource]:
        return [
            # Run first so it's available for Debezium, which gives up too quickly otherwise
            *redpanda_resources(apply_node_selectors=self.apply_node_selectors),
            *cockroach_resources(apply_node_selectors=self.apply_node_selectors),
            *postgres_resources(apply_node_selectors=self.apply_node_selectors),
            *mysql_resources(apply_node_selectors=self.apply_node_selectors),
            *debezium_resources(apply_node_selectors=self.apply_node_selectors),
            *ssh_resources(apply_node_selectors=self.apply_node_selectors),
            Minio(apply_node_selectors=self.apply_node_selectors),
            VpcEndpointsClusterRole(),
            #AdminRoleBinding(),
            #self.secret,
            #self.listeners_configmap,
            ## TODO install Materialize CR instead
            #EnvironmentdStatefulSet(
            #    release_mode=self.release_mode,
            #    tag=self.tag,
            #    log_filter=log_filter,
            #    coverage_mode=self.coverage_mode(),
            #    apply_node_selectors=self.apply_node_selectors,
            #),
            #PersistPubSubService(),
            self.environmentd,
            #self.materialized_alias,
            self.backend_secret,
            self.materialize,
        ]

    def get_images(self) -> list[str]:
        #return ["orchestratord", "environmentd", "clusterd", "testdrive", "postgres"]
        return ["testdrive", "postgres"]

    def register_vpc_endpoint(self) -> None:
        self.kubectl(
            "apply",
            "-f",
            os.path.join(
                os.path.abspath(self.mz_root),
                "src/cloud-resources/src/crd/generated/vpcendpoints.json",
            ),
        )

    def start_metrics_server(self) -> None:
        self.kubectl(
            "apply",
            "-f",
            "https://github.com/kubernetes-sigs/metrics-server/releases/download/metrics-server-helm-chart-3.8.2/components.yaml",
            namespace=None,
        )
        self.kubectl(
            "patch",
            "deployment",
            "metrics-server",
            "--namespace",
            "kube-system",
            "--type",
            "json",
            "-p",
            '[{"op": "add", "path": "/spec/template/spec/containers/0/args/-", "value": "--kubelet-insecure-tls" }]',
            namespace=None,
        )

    def wait_resource_creation_completed(self) -> None:
        wait(
            condition="condition=Ready",
            resource="pod",
            label="cluster.environmentd.materialize.cloud/cluster-id=u1",
        )

    def wait_replicas(self) -> None:
        for cluster_id in ("u1", "s1", "s2"):
            wait(
                condition="condition=Ready",
                resource="pod",
                label=f"cluster.environmentd.materialize.cloud/cluster-id={cluster_id}",
            )

    def wait_for_sql(self) -> None:
        """Wait until environmentd pod is ready and can accept SQL connections"""
        wait(condition="condition=Ready", resource="pod/mz{self.resource_id}-environmentd-{self.generation}-0")

        start = datetime.now()
        while datetime.now() - start < timedelta(seconds=300):
            try:
                self.environmentd.sql("SELECT 1")
                break
            except InterfaceError as e:
                # Since we crash environmentd, we expect some errors that we swallow.
                LOGGER.info(f"SQL interface not ready, {e} while SELECT 1. Waiting...")
                time.sleep(2)

    def set_environmentd_failpoints(self, failpoints: str) -> None:
        """Set the FAILPOINTS environmentd variable in the stateful set. This
        will most likely restart environmentd"""
        stateful_set = [
            resource
            for resource in self.resources
            if type(resource) == EnvironmentdStatefulSet
        ]
        assert len(stateful_set) == 1
        stateful_set = stateful_set[0]

        stateful_set.env["FAILPOINTS"] = failpoints
        stateful_set.replace()
        self.wait_for_sql()

    def get_k8s_value(
        self, selector: str, json_path: str, remove_quotes: bool = True
    ) -> str:
        value = self.kubectl(
            "get",
            "pods",
            f"--selector={selector}",
            "-o",
            f"jsonpath='{json_path}'",
        )

        if remove_quotes:
            value = value.replace("'", "")

        return value

    def get_pod_value(
        self, cluster_id: str, json_path: str, remove_quotes: bool = True
    ) -> str:
        return self.get_k8s_value(
            f"cluster.environmentd.materialize.cloud/cluster-id={cluster_id}",
            json_path,
            remove_quotes,
        )

    def get_pod_label_value(
        self, cluster_id: str, label: str, remove_quotes: bool = True
    ) -> str:
        return self.get_pod_value(
            cluster_id, "{.items[*].metadata.labels." + label + "}", remove_quotes
        )

    def get_cluster_node_names(self, cluster_name: str) -> list[str]:
        cluster_id = self.get_cluster_id(cluster_name)
        print(f"Cluster with name '{cluster_name}' has ID {cluster_id}")

        value_string = self.get_pod_value(
            cluster_id, "{.items[*].spec.nodeName}", remove_quotes=True
        )
        values = value_string.split(" ")
        return values

    def get_cluster_id(self, cluster_name: str) -> str:
        cluster_id: str = self.environmentd.sql_query(
            f"SELECT id FROM mz_clusters WHERE name = '{cluster_name}'"
        )[0][0]
        return cluster_id

    def get_cluster_and_replica_id(self, mz_table: str, name: str) -> tuple[str, str]:
        [cluster_id, replica_id] = self.environmentd.sql_query(
            f"SELECT s.cluster_id, r.id FROM {mz_table} s JOIN mz_cluster_replicas r ON r.cluster_id = s.cluster_id WHERE s.name = '{name}'"
        )[0]
        return cluster_id, replica_id

    def suspend_k8s_node(self, node_name: str) -> None:
        print(f"Suspending node {node_name}...")
        result = subprocess.run(
            ["docker", "pause", node_name], stderr=subprocess.STDOUT, text=True
        )

        assert result.returncode == 0, f"Got return code {result.returncode}"

        print(f"Suspended node {node_name}.")

    def revive_suspended_k8s_node(self, node_name: str) -> None:
        print(f"Reviving node {node_name}...")
        result = subprocess.run(
            ["docker", "unpause", node_name], stderr=subprocess.STDOUT, text=True
        )

        assert result.returncode == 0, f"Got return code {result.returncode}"

        print(f"Node {node_name} is running again.")

    def coverage_mode(self) -> bool:
        return ui.env_is_truthy("CI_COVERAGE_ENABLED")

    def sanitizer_mode(self) -> str:
        return os.getenv("CI_SANITIZER", "none")

    def bazel(self) -> bool:
        return ui.env_is_truthy("CI_BAZEL_BUILD")

    def bazel_remote_cache(self) -> str | None:
        return os.getenv("CI_BAZEL_REMOTE_CACHE")

    def install_orchestratord(self, values_file: str | None = None) -> None:
        args = [
            "--kind-cluster-name",
            self.cluster_name(),
            "run",
            # TODO make this optional?
            "--dev",
        ]

        if values_file is not None:
            args.extend(["--values", values_file])

        orchestratord.main(args)

    def run_materialize(self) -> None:
        args = [
            "--kind-cluster-name",
            self.cluster_name(),
            "environment",
            # TODO make this optional?
            "--dev",
        ]

        orchestratord.main(args)

    def kubectl(self, *args: str, namespace: str | None = DEFAULT_K8S_NAMESPACE) -> str:
        try:
            cmd = ["kubectl", "--context", self.context()]

            if namespace is not None:
                cmd.extend(["--namespace", namespace])

            cmd.extend(args)

            return subprocess.check_output(cmd, text=True)
        except subprocess.CalledProcessError as e:
            log_subprocess_error(e)
            raise e

    def context(self) -> str:
        return DEFAULT_K8S_CONTEXT_NAME

    def cluster_name(self) -> str:
        return DEFAULT_K8S_CLUSTER_NAME

    def acquire_images(self) -> None:
        repo = mzbuild.Repository(
            self.mz_root,
            profile=(
                mzbuild.Profile.RELEASE if self.release_mode else mzbuild.Profile.DEV
            ),
            coverage=self.coverage_mode(),
            bazel=self.bazel(),
            bazel_remote_cache=self.bazel_remote_cache(),
        )
        for image in self.images:
            self._acquire_image(repo, image)

    def _acquire_image(self, repo: mzbuild.Repository, image: str) -> None:
        deps = repo.resolve_dependencies([repo.images[image]])
        deps.acquire()
        for dep in deps:
            subprocess.check_call(
                [
                    "kind",
                    "load",
                    "docker-image",
                    f"--name={self.cluster_name()}",
                    dep.spec(),
                ]
            )

    def create_namespace(self) -> None:
        try:
            self.kubectl(
                "get",
                "namespace",
                DEFAULT_K8S_NAMESPACE,
                namespace=None,
            )
        except:
            self.kubectl(
                "create",
                "namespace",
                DEFAULT_K8S_NAMESPACE,
                namespace=None,
            )

    def create_resources(self) -> None:
        for resource in self.resources:
            resource.create()
