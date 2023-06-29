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
from semver import Version

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


class MaterializedAliasService(K8sService):
    """Some testdrive tests expect that Mz is accessible as 'materialized'"""

    def __init__(self) -> None:
        self.service = V1Service(
            api_version="v1",
            kind="Service",
            metadata=V1ObjectMeta(name="materialized"),
            spec=V1ServiceSpec(
                type="ExternalName",
                external_name="environmentd.default.svc.cluster.local",
            ),
        )


class EnvironmentdStatefulSet(K8sStatefulSet):
    def __init__(
        self,
        tag: Optional[str] = None,
        release_mode: bool = True,
        coverage_mode: bool = False,
        log_filter: Optional[str] = None,
    ) -> None:
        self.tag = tag
        self.release_mode = release_mode
        self.coverage_mode = coverage_mode
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
            V1EnvVar(
                name="MZ_AWS_PRIVATELINK_AVAILABILITY_ZONES", value="use1-az1,use1-az2"
            ),
            # TODO: these should be the same as in mzcompose
            V1EnvVar(
                name="MZ_SYSTEM_PARAMETER_DEFAULT",
                value="enable_envelope_upsert_in_subscribe=true",
            ),
            V1EnvVar(
                name="MZ_SYSTEM_PARAMETER_DEFAULT",
                value="enable_disk_cluster_replicas=true",
            ),
        ]

        if self.coverage_mode:
            env.extend(
                [
                    V1EnvVar(
                        name="LLVM_PROFILE_FILE",
                        value="/coverage/environmentd-%p-%9m%c.profraw",
                    ),
                    V1EnvVar(
                        name="CI_COVERAGE_ENABLED",
                        value="1",
                    ),
                    V1EnvVar(name="MZ_ORCHESTRATOR_KUBERNETES_COVERAGE", value="1"),
                ]
            )

        for (k, v) in self.env.items():
            env.append(V1EnvVar(name=k, value=v))

        ports = [V1ContainerPort(container_port=5432, name="sql")]

        volume_mounts = [V1VolumeMount(name="data", mount_path="/data")]

        if self.coverage_mode:
            volume_mounts.append(V1VolumeMount(name="coverage", mount_path="/coverage"))

        s3_endpoint = urllib.parse.quote("http://minio-service.default:9000")

        args = [
            "--availability-zone=1",
            "--availability-zone=2",
            "--availability-zone=3",
            "--aws-account-id=123456789000",
            "--aws-external-id-prefix=eb5cb59b-e2fe-41f3-87ca-d2176a495345",
            "--announce-egress-ip=1.2.3.4",
            "--announce-egress-ip=88.77.66.55",
            "--environment-id=cloudtest-test-00000000-0000-0000-0000-000000000000-0",
            f"--persist-blob-url=s3://minio:minio123@persist/persist?endpoint={s3_endpoint}&region=minio",
            "--orchestrator=kubernetes",
            "--orchestrator-kubernetes-image-pull-policy=if-not-present",
            # Kind sets up a basic local-file storage class based on Rancher, named `standard`
            "--orchestrator-kubernetes-ephemeral-volume-class=standard",
            "--persist-consensus-url=postgres://root@cockroach.default:26257?options=--search_path=consensus",
            "--adapter-stash-url=postgres://root@cockroach.default:26257?options=--search_path=adapter",
            "--storage-stash-url=postgres://root@cockroach.default:26257?options=--search_path=storage",
            "--internal-sql-listen-addr=0.0.0.0:6877",
            "--unsafe-mode",
            # cloudtest may be called upon to spin up older versions of
            # Materialize too! If you are adding a command-line option that is
            # only supported on newer releases, do not add it here. Add it as a
            # version-gated argument below, using `self._meets_minimum_version`.
        ]
        if self.log_filter:
            args += [f"--log-filter={self.log_filter}"]
        if self._meets_minimum_version("0.38.0"):
            args += [
                "--clusterd-image",
                self.image(
                    "clusterd",
                    tag=self.tag,
                    release_mode=self.release_mode,
                ),
            ]
        else:
            args += [
                "--storaged-image",
                self.image(
                    "storaged",
                    tag=self.tag,
                    release_mode=self.release_mode,
                ),
                "--computed-image",
                self.image(
                    "computed",
                    tag=self.tag,
                    release_mode=self.release_mode,
                ),
            ]
        if self._meets_minimum_version("0.53.0"):
            args += [
                "--bootstrap-role",
                "materialize",
            ]
        if self._meets_minimum_version("0.54.0"):
            args += [
                "--internal-persist-pubsub-listen-addr=0.0.0.0:6879",
                "--persist-pubsub-url=http://persist-pubsub",
            ]
        container = V1Container(
            name="environmentd",
            image=self.image(
                "environmentd",
                tag=self.tag,
                release_mode=self.release_mode,
            ),
            args=args,
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
            ),
        ]

        if self.coverage_mode:
            claim_templates.append(
                V1PersistentVolumeClaim(
                    metadata=V1ObjectMeta(name="coverage"),
                    spec=V1PersistentVolumeClaimSpec(
                        access_modes=["ReadWriteOnce"],
                        resources=V1ResourceRequirements(requests={"storage": "10Gi"}),
                    ),
                )
            )

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

    def _meets_minimum_version(self, minimum: str) -> bool:
        """Determine whether environmentd is at least the `minimum` version.

        This function matches the function of the same name in MaterializeInc/cloud.
        """

        # Assume that unstable and development versions, as indicated by a
        # missing or unparseable tag, are always recent enough to support all
        # features.
        #
        # TODO: learn how to do real feature detection on arbitrary versions.
        if self.tag is None:
            return True
        try:
            tag_version = Version.parse(self.tag.removeprefix("v"))
        except ValueError:
            return True

        minimum_version = Version.parse(minimum)
        return tag_version >= minimum_version
