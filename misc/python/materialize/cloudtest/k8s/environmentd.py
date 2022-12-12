# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import urllib.parse
from typing import Dict, Optional

from kubernetes.client import (
    V1Container,
    V1ContainerPort,
    V1EnvVar,
    V1EnvVarSource,
    V1LabelSelector,
    V1ObjectFieldSelector,
    V1ObjectMeta,
    V1PersistentVolumeClaim,
    V1PersistentVolumeClaimSpec,
    V1PodSpec,
    V1PodTemplateSpec,
    V1ResourceRequirements,
    V1Service,
    V1ServicePort,
    V1ServiceSpec,
    V1StatefulSet,
    V1StatefulSetSpec,
    V1VolumeMount,
)

from materialize.cloudtest.k8s import K8sService, K8sStatefulSet


class EnvironmentdService(K8sService):
    def __init__(self) -> None:
        service_port = V1ServicePort(name="sql", port=6875)
        internal_port = V1ServicePort(name="internal", port=6877)
        self.service = V1Service(
            api_version="v1",
            kind="Service",
            metadata=V1ObjectMeta(name="environmentd", labels={"app": "environmentd"}),
            spec=V1ServiceSpec(
                type="NodePort",
                ports=[service_port, internal_port],
                selector={"app": "environmentd"},
            ),
        )


class EnvironmentdStatefulSet(K8sStatefulSet):
    def __init__(
        self,
        tag: Optional[str] = None,
        release_mode: bool = True,
        log_filter: Optional[str] = None,
    ) -> None:
        self.tag = tag
        self.release_mode = release_mode
        self.log_filter = log_filter
        self.env: Dict[str, str] = {}
        super().__init__()

    def generate_stateful_set(self) -> V1StatefulSet:
        metadata = V1ObjectMeta(name="environmentd", labels={"app": "environmentd"})
        label_selector = V1LabelSelector(match_labels={"app": "environmentd"})

        value_from = V1EnvVarSource(
            field_ref=V1ObjectFieldSelector(field_path="metadata.name")
        )

        env = [
            V1EnvVar(name="MZ_POD_NAME", value_from=value_from),
            V1EnvVar(name="AWS_REGION", value="minio"),
            V1EnvVar(name="AWS_ACCESS_KEY_ID", value="minio"),
            V1EnvVar(name="AWS_SECRET_ACCESS_KEY", value="minio123"),
            V1EnvVar(name="MZ_ANNOUNCE_EGRESS_IP", value="1.2.3.4,88.77.66.55"),
            V1EnvVar(name="MZ_AWS_ACCOUNT_ID", value="123456789000"),
            V1EnvVar(
                name="MZ_AWS_EXTERNAL_ID_PREFIX",
                value="eb5cb59b-e2fe-41f3-87ca-d2176a495345",
            ),
        ]

        for (k, v) in self.env.items():
            env.append(V1EnvVar(name=k, value=v))

        ports = [V1ContainerPort(container_port=5432, name="sql")]

        volume_mounts = [
            V1VolumeMount(name="data", mount_path="/data"),
        ]

        s3_endpoint = urllib.parse.quote("http://minio-service.default:9000")

        container = V1Container(
            name="environmentd",
            image=self.image(
                "environmentd", tag=self.tag, release_mode=self.release_mode
            ),
            args=[
                "--storaged-image="
                + self.image("storaged", tag=self.tag, release_mode=self.release_mode),
                "--computed-image="
                + self.image("computed", tag=self.tag, release_mode=self.release_mode),
                "--availability-zone=kind-worker",
                "--availability-zone=kind-worker2",
                "--availability-zone=kind-worker3",
                "--environment-id=cloudtest-test-00000000-0000-0000-0000-000000000000-0",
                f"--persist-blob-url=s3://minio:minio123@persist/persist?endpoint={s3_endpoint}&region=minio",
                "--orchestrator=kubernetes",
                "--orchestrator-kubernetes-image-pull-policy=if-not-present",
                "--persist-consensus-url=postgres://postgres@postgres.default?options=--search_path=consensus",
                "--adapter-stash-url=postgres://postgres@postgres.default?options=--search_path=catalog",
                "--storage-stash-url=postgres://postgres@postgres.default?options=--search_path=storage",
                "--internal-sql-listen-addr=0.0.0.0:6877",
                "--unsafe-mode",
                # cloudtest may be called upon to spin up older versions of Mz too!
                # If you are adding a command-line option that is only supported on newer
                # releases, do not add it here, add it as a V1EnvVar above instead.
            ]
            + ([f"--log-filter={self.log_filter}"] if self.log_filter else []),
            env=env,
            ports=ports,
            volume_mounts=volume_mounts,
        )

        pod_spec = V1PodSpec(containers=[container])
        template_spec = V1PodTemplateSpec(metadata=metadata, spec=pod_spec)
        claim_templates = [
            V1PersistentVolumeClaim(
                metadata=V1ObjectMeta(name="data"),
                spec=V1PersistentVolumeClaimSpec(
                    access_modes=["ReadWriteOnce"],
                    resources=V1ResourceRequirements(requests={"storage": "1Gi"}),
                ),
            )
        ]

        return V1StatefulSet(
            api_version="apps/v1",
            kind="StatefulSet",
            metadata=metadata,
            spec=V1StatefulSetSpec(
                service_name="environmentd",
                replicas=1,
                pod_management_policy="Parallel",
                selector=label_selector,
                template=template_spec,
                volume_claim_templates=claim_templates,
            ),
        )
