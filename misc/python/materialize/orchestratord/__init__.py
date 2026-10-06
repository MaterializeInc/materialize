# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Client for a Materialize environment managed by orchestratord.

Talks to the Kubernetes API directly through the `kubernetes` client, so it
runs inside a pod with in-cluster credentials as well as from a developer
machine with a kubeconfig. Only `materialize.cloud/v1alpha1` is used: it is the
stored version, so reads never go through the conversion webhook.
"""

from __future__ import annotations

import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import requests
from kubernetes import client  # type: ignore
from kubernetes.client.rest import ApiException  # type: ignore
from kubernetes.config.config_exception import ConfigException
from kubernetes.config.incluster_config import load_incluster_config
from kubernetes.config.kube_config import load_kube_config

GROUP = "materialize.cloud"
VERSION = "v1alpha1"
PLURAL = "materializes"
CRD_NAME = f"{PLURAL}.{GROUP}"
LEADER_STATUS_PORT = 6878


def load_config() -> None:
    """Load in-cluster credentials, falling back to the local kubeconfig."""
    try:
        load_incluster_config()
    except ConfigException:
        load_kube_config()


@dataclass(frozen=True)
class EnvironmentdGeneration:
    generation: int
    statefulset_name: str
    statefulset_uid: str
    image: str
    force: str | None
    """Value of the `materialize.cloud/force` annotation on the StatefulSet."""


@dataclass(frozen=True)
class EnvironmentdPod:
    generation: int
    name: str
    uid: str
    ip: str | None
    restart_count: int
    ready: bool


class Materialize:
    """One Materialize custom resource and the resources orchestratord derives from it."""

    def __init__(self, namespace: str, name: str) -> None:
        self.namespace = namespace
        self.name = name
        self.custom = client.CustomObjectsApi()
        self.apps = client.AppsV1Api()
        self.core = client.CoreV1Api()

    def get(self) -> dict[str, Any] | None:
        try:
            return self.custom.get_namespaced_custom_object(
                GROUP, VERSION, self.namespace, PLURAL, self.name
            )
        except ApiException as e:
            if e.status == 404:
                return None
            raise

    def create(self, body: dict[str, Any]) -> None:
        self.custom.create_namespaced_custom_object(
            GROUP, VERSION, self.namespace, PLURAL, body
        )

    def patch_spec(self, spec: dict[str, Any]) -> dict[str, Any]:
        """Merge-patch the spec and return the resulting object."""
        return self.custom.patch_namespaced_custom_object(
            GROUP, VERSION, self.namespace, PLURAL, self.name, {"spec": spec}
        )

    def request_rollout(
        self, spec: dict[str, Any] | None = None, request_id: str | None = None
    ) -> str:
        """Request a rollout, optionally changing the spec in the same patch.

        Sets `requestRollout` and `forceRollout` to one UUID, which makes
        orchestratord create a new generation even when nothing else changed.
        Returns the request id. Callers that need reproducible ids (such as
        Antithesis drivers) pass their own.
        """
        request = request_id or str(uuid.uuid4())
        self.patch_spec(
            {**(spec or {}), "requestRollout": request, "forceRollout": request}
        )
        return request

    def status(self) -> dict[str, Any]:
        mz = self.get()
        return (mz or {}).get("status") or {}

    def resource_id(self) -> str | None:
        return self.status().get("resourceId")

    def condition(self) -> dict[str, Any] | None:
        """The `UpToDate` condition, whose reason encodes the rollout phase."""
        for c in self.status().get("conditions") or []:
            if c.get("type") == "UpToDate":
                return c
        return None

    def phase(self) -> str | None:
        c = self.condition()
        return c.get("reason") if c else None

    def is_up_to_date(self) -> bool:
        mz = self.get()
        if mz is None:
            return False
        status = mz.get("status") or {}
        c = next(
            (c for c in status.get("conditions") or [] if c.get("type") == "UpToDate"),
            None,
        )
        return (
            c is not None
            and c.get("status") == "True"
            and status.get("lastCompletedRolloutRequest")
            == mz["spec"].get("requestRollout")
        )

    def environmentd_generations(self) -> list[EnvironmentdGeneration]:
        rid = self.resource_id()
        if rid is None:
            return []
        prefix = f"mz{rid}-environmentd-"
        result = []
        for sts in self.apps.list_namespaced_stateful_set(self.namespace).items:
            assert sts.metadata is not None
            name = sts.metadata.name
            assert name is not None
            suffix = name.removeprefix(prefix)
            if suffix == name or not suffix.isdigit():
                continue
            assert sts.metadata.uid is not None and sts.spec is not None
            template = sts.spec.template
            assert template.spec is not None
            annotations = sts.metadata.annotations or {}
            image = template.spec.containers[0].image
            assert image is not None
            result.append(
                EnvironmentdGeneration(
                    generation=int(suffix),
                    statefulset_name=name,
                    statefulset_uid=sts.metadata.uid,
                    image=image,
                    force=annotations.get("materialize.cloud/force"),
                )
            )
        return sorted(result, key=lambda g: g.generation)

    def environmentd_pods(self) -> list[EnvironmentdPod]:
        pods = self.core.list_namespaced_pod(
            self.namespace, label_selector="materialize.cloud/app=environmentd"
        ).items
        result = []
        for pod in pods:
            metadata = pod.metadata
            assert metadata is not None
            generation = (metadata.annotations or {}).get(
                "materialize.cloud/generation"
            )
            if generation is None:
                continue
            assert metadata.name is not None and metadata.uid is not None
            assert pod.status is not None
            statuses = pod.status.container_statuses or []
            result.append(
                EnvironmentdPod(
                    generation=int(generation),
                    name=metadata.name,
                    uid=metadata.uid,
                    ip=pod.status.pod_ip,
                    restart_count=sum(s.restart_count for s in statuses),
                    ready=bool(statuses) and all(s.ready for s in statuses),
                )
            )
        return result

    def clusterd_statefulset_generations(self) -> dict[str, int]:
        """clusterd StatefulSet name to the deploy generation in its name."""
        result = {}
        for sts in self.apps.list_namespaced_stateful_set(self.namespace).items:
            assert sts.metadata is not None
            name = sts.metadata.name
            assert name is not None
            head, sep, tail = name.rpartition("-gen-")
            if sep and tail.isdigit() and "environmentd" not in head:
                result[name] = int(tail)
        return result


def leader_status(pod_ip: str, timeout: float = 5.0) -> str | None:
    """`/api/leader/status` of one environmentd incarnation, or None if unreachable.

    Addresses the pod directly rather than a Service, because a generation
    Service only routes to Ready pods and leadership must be observed per
    incarnation.
    """
    try:
        response = requests.get(
            f"http://{pod_ip}:{LEADER_STATUS_PORT}/api/leader/status",
            timeout=timeout,
        )
        response.raise_for_status()
        return response.json().get("status")
    except (requests.RequestException, ValueError):
        return None


def crd_established() -> bool:
    try:
        crd = client.ApiextensionsV1Api().read_custom_resource_definition(CRD_NAME)
    except ApiException as e:
        if e.status == 404:
            return False
        raise
    assert crd.status is not None
    return any(
        c.type == "Established" and c.status == "True"
        for c in (crd.status.conditions or [])
    )


def wait_until(
    predicate: Callable[[], bool],
    timeout: float,
    interval: float = 2.0,
    description: str = "condition",
) -> None:
    """Poll `predicate` until it holds, swallowing transient API errors."""
    deadline = time.monotonic() + timeout
    while True:
        try:
            if predicate():
                return
        except (ApiException, requests.RequestException, OSError):
            pass
        if time.monotonic() >= deadline:
            raise TimeoutError(f"timed out after {timeout}s waiting for {description}")
        time.sleep(interval)
