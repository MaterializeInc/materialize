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

from pg8000.exceptions import InterfaceError

from materialize.cloudtest.app.cloudtest_application_base import (
    CloudtestApplicationBase,
)
from materialize.cloudtest.k8s.api.k8s_resource import K8sResource
from materialize.cloudtest.k8s.cockroach import cockroach_resources
from materialize.cloudtest.k8s.debezium import debezium_resources
from materialize.cloudtest.k8s.environmentd import (
    EnvironmentdService,
    EnvironmentdStatefulSet,
    MaterializedAliasService,
)
from materialize.cloudtest.k8s.minio import Minio
from materialize.cloudtest.k8s.mysql import mysql_resources
from materialize.cloudtest.k8s.persist_pubsub import PersistPubSubService
from materialize.cloudtest.k8s.postgres import postgres_resources
from materialize.cloudtest.k8s.redpanda import redpanda_resources
from materialize.cloudtest.k8s.role_binding import AdminRoleBinding
from materialize.cloudtest.k8s.ssh import ssh_resources
from materialize.cloudtest.k8s.testdrive import TestdrivePod
from materialize.cloudtest.k8s.vpc_endpoints_cluster_role import VpcEndpointsClusterRole
from materialize.cloudtest.util.wait import wait

LOGGER = logging.getLogger(__name__)


class MaterializeApplication(CloudtestApplicationBase):
    def __init__(
        self,
        release_mode: bool = True,
        tag: str | None = None,
        aws_region: str | None = None,
        log_filter: str | None = None,
        apply_node_selectors: bool = False,
    ) -> None:
        self.tag = tag
        self.environmentd = EnvironmentdService()
        self.materialized_alias = MaterializedAliasService()
        self.testdrive = TestdrivePod(
            release_mode=release_mode,
            aws_region=aws_region,
            apply_node_selectors=apply_node_selectors,
        )
        self.apply_node_selectors = apply_node_selectors
        super().__init__(release_mode, aws_region, log_filter)

        # Register the VpcEndpoint CRD.
        self.register_vpc_endpoint()

        self.start_metrics_server()

        self.create_resources_and_wait()

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
            AdminRoleBinding(),
            EnvironmentdStatefulSet(
                release_mode=self.release_mode,
                tag=self.tag,
                log_filter=log_filter,
                coverage_mode=self.coverage_mode(),
                apply_node_selectors=self.apply_node_selectors,
            ),
            PersistPubSubService(),
            self.environmentd,
            self.materialized_alias,
            self.testdrive,
        ]

    def get_images(self) -> list[str]:
        return ["environmentd", "clusterd", "testdrive", "postgres"]

    def register_vpc_endpoint(self) -> None:
        self.kubectl(
            "apply",
            "-f",
            os.path.join(
                os.path.abspath(self.mz_root),
                "src/cloud-resources/src/crd/gen/vpcendpoints.json",
            ),
        )

    def start_metrics_server(self) -> None:
        self.kubectl(
            "apply",
            "-f",
            "https://github.com/kubernetes-sigs/metrics-server/releases/download/metrics-server-helm-chart-3.8.2/components.yaml",
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
        wait(condition="condition=Ready", resource="pod/environmentd-0")

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
