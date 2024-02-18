# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import operator
import urllib.parse
from collections.abc import Callable

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
    V1Toleration,
    V1VolumeMount,
)

from materialize.cloudtest import DEFAULT_K8S_NAMESPACE
from materialize.cloudtest.k8s.api.k8s_service import K8sService
from materialize.cloudtest.k8s.api.k8s_stateful_set import K8sStatefulSet
from materialize.mz_version import MzVersion
from materialize.mzcompose import DEFAULT_SYSTEM_PARAMETERS


class EnvironmentdService(K8sService):
    def __init__(self, namespace: str = DEFAULT_K8S_NAMESPACE) -> None:
        super().__init__(namespace)
        service_port = V1ServicePort(name="sql", port=6875)
        http_port = V1ServicePort(name="http", port=6876)
        internal_port = V1ServicePort(name="internal", port=6877)
        internal_http_port = V1ServicePort(name="internalhttp", port=6878)
        self.service = V1Service(
            api_version="v1",
            kind="Service",
            metadata=V1ObjectMeta(name="environmentd", labels={"app": "environmentd"}),
            spec=V1ServiceSpec(
                type="NodePort",
                ports=[service_port, internal_port, http_port, internal_http_port],
                selector={"app": "environmentd"},
            ),
        )


class MaterializedAliasService(K8sService):
    """Some testdrive tests expect that Mz is accessible as 'materialized'"""

    def __init__(self, namespace: str = DEFAULT_K8S_NAMESPACE) -> None:
        super().__init__(namespace)
        self.service = V1Service(
            api_version="v1",
            kind="Service",
            metadata=V1ObjectMeta(name="materialized"),
            spec=V1ServiceSpec(
                type="ExternalName",
                external_name=f"environmentd.{namespace}.svc.cluster.local",
            ),
        )


class EnvironmentdStatefulSet(K8sStatefulSet):
    def __init__(
        self,
        tag: str | None = None,
        release_mode: bool = True,
        coverage_mode: bool = False,
        log_filter: str | None = None,
        namespace: str = DEFAULT_K8S_NAMESPACE,
        minio_namespace: str = DEFAULT_K8S_NAMESPACE,
        cockroach_namespace: str = DEFAULT_K8S_NAMESPACE,
        apply_node_selectors: bool = False,
    ) -> None:
        self.tag = tag
        self.release_mode = release_mode
        self.coverage_mode = coverage_mode
        self.log_filter = log_filter
        self.env: dict[str, str] = {}
        self.extra_args: list[str] = []
        self.minio_namespace = minio_namespace
        self.cockroach_namespace = cockroach_namespace
        self.apply_node_selectors = apply_node_selectors
        super().__init__(namespace)

    def generate_stateful_set(self) -> V1StatefulSet:
        metadata = V1ObjectMeta(name="environmentd", labels={"app": "environmentd"})
        label_selector = V1LabelSelector(match_labels={"app": "environmentd"})

        ports = [V1ContainerPort(container_port=5432, name="sql")]

        volume_mounts = []

        if self.coverage_mode:
            volume_mounts.append(V1VolumeMount(name="coverage", mount_path="/coverage"))

        container = V1Container(
            name="environmentd",
            image=self.image(
                "environmentd",
                tag=self.tag,
                release_mode=self.release_mode,
            ),
            args=self.args(),
            env=self.env_vars(),
            ports=ports,
            volume_mounts=volume_mounts,
        )

        node_selector = None
        if self.apply_node_selectors:
            node_selector = {"environmentd": "true"}

        taint_toleration = V1Toleration(
            key="environmentd",
            operator="Equal",
            value="true",
            effect="NoSchedule",
        )

        pod_spec = V1PodSpec(
            containers=[container],
            tolerations=[taint_toleration],
            node_selector=node_selector,
            termination_grace_period_seconds=0,
        )
        template_spec = V1PodTemplateSpec(metadata=metadata, spec=pod_spec)

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
                volume_claim_templates=self.claim_templates(),
            ),
        )

    def claim_templates(self) -> list[V1PersistentVolumeClaim]:
        claim_templates = []

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

        return claim_templates

    def args(self) -> list[str]:
        s3_endpoint = urllib.parse.quote(
            f"http://minio-service.{self.minio_namespace}:9000"
        )

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
            f"--persist-consensus-url=postgres://root@cockroach.{self.cockroach_namespace}:26257?options=--search_path=consensus",
            f"--adapter-stash-url=postgres://root@cockroach.{self.cockroach_namespace}:26257?options=--search_path=adapter",
            f"--storage-stash-url=postgres://root@cockroach.{self.cockroach_namespace}:26257?options=--search_path=storage",
            "--internal-sql-listen-addr=0.0.0.0:6877",
            "--internal-http-listen-addr=0.0.0.0:6878",
            "--unsafe-mode",
            # cloudtest may be called upon to spin up older versions of
            # Materialize too! If you are adding a command-line option that is
            # only supported on newer releases, do not add it here. Add it as a
            # version-gated argument below, using `self._meets_minimum_version`.
        ]
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
        if self._meets_minimum_version("0.60.0-dev"):
            args += [
                # Kind sets up a basic local-file storage class based on Rancher, named `standard`
                "--orchestrator-kubernetes-ephemeral-volume-class=standard"
            ]
        if self._meets_minimum_version("0.63.0-dev"):
            args += ["--secrets-controller=kubernetes"]

        if self._meets_minimum_version("0.79.0-dev"):
            args += [
                f"--timestamp-oracle-url=postgres://root@cockroach.{self.cockroach_namespace}:26257?options=--search_path=tsoracle"
            ]

        return args + self.extra_args

    def env_vars(self) -> list[V1EnvVar]:

        system_parameter_defaults = DEFAULT_SYSTEM_PARAMETERS

        if self.log_filter:
            system_parameter_defaults["log_filter"] = self.log_filter
        if self._meets_maximum_version("0.63.99"):
            system_parameter_defaults["enable_managed_clusters"] = "true"

        value_from = V1EnvVarSource(
            field_ref=V1ObjectFieldSelector(field_path="metadata.name")
        )

        env = [
            V1EnvVar(name="MZ_SOFT_ASSERTIONS", value="1"),
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
            V1EnvVar(
                name="MZ_AWS_CONNECTION_ROLE_ARN",
                value="arn:aws:iam::123456789000:role/MaterializeConnection",
            ),
            V1EnvVar(
                name="MZ_SYSTEM_PARAMETER_DEFAULT",
                value=";".join(
                    [
                        f"{key}={value}"
                        for key, value in system_parameter_defaults.items()
                    ]
                ),
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

        for k, v in self.env.items():
            env.append(V1EnvVar(name=k, value=v))

        return env

    def _meets_version(self, version: str, operator: Callable, default: bool) -> bool:
        """Determine whether environmentd matches a given version based on a comparison operator"""

        if self.tag is None:
            return default
        try:
            tag_version = MzVersion.parse_mz(self.tag)
        except ValueError:
            return default

        cmp_version = MzVersion.parse_without_prefix(version)
        return bool(operator(tag_version, cmp_version))

    def _meets_minimum_version(self, version: str) -> bool:
        return self._meets_version(version=version, operator=operator.ge, default=True)

    def _meets_maximum_version(self, version: str) -> bool:
        return self._meets_version(version=version, operator=operator.le, default=False)
