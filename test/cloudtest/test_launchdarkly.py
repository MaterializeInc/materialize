# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Regression test for LaunchDarkly SSE stream disruption and recovery.

Deploys a Toxiproxy inside K8s that TCP-proxies to stream.launchdarkly.com.
The environmentd pod's /etc/hosts maps LD hostnames to the Toxiproxy ClusterIP
so all LD traffic flows through the proxy transparently (TLS passes through).

The test verifies that:
  1. Flag sync works through the proxy
  2. Disabling the proxy stops sync (value doesn't change)
  3. Re-enabling the proxy restores sync (regression gate)

Requires LAUNCHDARKLY_SDK_KEY and LAUNCHDARKLY_API_TOKEN env vars.
"""

import copy
import json
import os
import time
from typing import Any
from uuid import uuid1

import launchdarkly_api  # type: ignore
import pytest
import requests
from kubernetes.client import (
    V1Container,
    V1ContainerPort,
    V1Deployment,
    V1DeploymentSpec,
    V1HostAlias,
    V1LabelSelector,
    V1ObjectMeta,
    V1PodSpec,
    V1PodTemplateSpec,
    V1Service,
    V1ServicePort,
    V1ServiceSpec,
)
from launchdarkly_api.api import feature_flags_api  # type: ignore
from launchdarkly_api.model.client_side_availability_post import (  # type: ignore
    ClientSideAvailabilityPost,
)
from launchdarkly_api.model.defaults import Defaults  # type: ignore
from launchdarkly_api.model.feature_flag_body import FeatureFlagBody  # type: ignore
from launchdarkly_api.model.json_patch import JSONPatch  # type: ignore
from launchdarkly_api.model.patch_operation import PatchOperation  # type: ignore
from launchdarkly_api.model.patch_with_comment import PatchWithComment  # type: ignore
from launchdarkly_api.model.variation import Variation  # type: ignore

from materialize.cloudtest import DEFAULT_K8S_NAMESPACE
from materialize.cloudtest.app.materialize_application import MaterializeApplication
from materialize.cloudtest.k8s.api.k8s_deployment import K8sDeployment
from materialize.cloudtest.k8s.api.k8s_service import K8sService
from materialize.cloudtest.k8s.environmentd import EnvironmentdStatefulSet
from materialize.cloudtest.k8s.toxiproxy import TOXIPROXY_IMAGE
from materialize.cloudtest.util.wait import wait

LAUNCHDARKLY_API_TOKEN = os.environ.get("LAUNCHDARKLY_API_TOKEN")
LAUNCHDARKLY_SDK_KEY = os.environ.get("LAUNCHDARKLY_SDK_KEY")
BUILDKITE_JOB_ID = os.environ.get("BUILDKITE_JOB_ID", str(uuid1()))
BUILDKITE_PULL_REQUEST = os.environ.get("BUILDKITE_PULL_REQUEST")

LD_HOSTS = [
    "stream.launchdarkly.com",
    "sdk.launchdarkly.com",
    "events.launchdarkly.com",
    "app.launchdarkly.com",
    "clientsdk.launchdarkly.com",
]

TOXIPROXY_NAME = "toxiproxy-ld"
LD_PROXY_NAME = "ld-stream"


class LdToxiproxyDeployment(K8sDeployment):
    def __init__(self, namespace: str = DEFAULT_K8S_NAMESPACE) -> None:
        super().__init__(namespace)
        self.deployment = V1Deployment(
            api_version="apps/v1",
            kind="Deployment",
            metadata=V1ObjectMeta(name=TOXIPROXY_NAME, namespace=namespace),
            spec=V1DeploymentSpec(
                replicas=1,
                selector=V1LabelSelector(match_labels={"app": TOXIPROXY_NAME}),
                template=V1PodTemplateSpec(
                    metadata=V1ObjectMeta(labels={"app": TOXIPROXY_NAME}),
                    spec=V1PodSpec(
                        containers=[
                            V1Container(
                                name="toxiproxy",
                                image=TOXIPROXY_IMAGE,
                                args=["-host=0.0.0.0"],
                                ports=[
                                    V1ContainerPort(name="admin", container_port=8474),
                                    V1ContainerPort(
                                        name="ld-stream", container_port=443
                                    ),
                                ],
                            )
                        ]
                    ),
                ),
            ),
        )

    def delete(self) -> None:
        self.apps_api().delete_namespaced_deployment(
            name=TOXIPROXY_NAME, namespace=self.namespace()
        )


class LdToxiproxyService(K8sService):
    def __init__(self, namespace: str = DEFAULT_K8S_NAMESPACE) -> None:
        super().__init__(namespace)
        self.service = V1Service(
            metadata=V1ObjectMeta(
                name=TOXIPROXY_NAME,
                namespace=namespace,
                labels={"app": TOXIPROXY_NAME},
            ),
            spec=V1ServiceSpec(
                type="NodePort",
                selector={"app": TOXIPROXY_NAME},
                ports=[
                    V1ServicePort(name="admin", port=8474),
                    V1ServicePort(name="ld-stream", port=443),
                ],
            ),
        )

    def delete(self) -> None:
        self.api().delete_namespaced_service(
            name=TOXIPROXY_NAME, namespace=self.namespace()
        )


class LaunchDarklyClient:
    def __init__(self, api_token: str, project_key: str, environment_key: str) -> None:
        self.configuration = launchdarkly_api.Configuration(
            api_key=dict(ApiKey=api_token),
        )
        self.project_key = project_key
        self.environment_key = environment_key

    def create_flag(self, key: str, tags: list[str] | None = None) -> Any:
        with launchdarkly_api.ApiClient(self.configuration) as api_client:
            api = feature_flags_api.FeatureFlagsApi(api_client)
            return api.post_feature_flag(
                project_key=self.project_key,
                feature_flag_body=FeatureFlagBody(
                    name=key,
                    key=key,
                    client_side_availability=ClientSideAvailabilityPost(
                        using_environment_id=True, using_mobile_key=True
                    ),
                    variations=[
                        Variation(value=1073741824, name="1 GiB"),
                        Variation(value=2147483648, name="2 GiB"),
                        Variation(value=3221225472, name="3 GiB"),
                        Variation(value=4294967295, name="4 GiB - 1"),
                    ],
                    temporary=False,
                    tags=tags or [],
                    defaults=Defaults(off_variation=0, on_variation=1),
                ),
            )

    def update_targeting(
        self,
        key: str,
        on: bool | None = None,
        context_targets: list[Any] | None = None,
    ) -> Any:
        with launchdarkly_api.ApiClient(self.configuration) as api_client:
            api = feature_flags_api.FeatureFlagsApi(api_client)
            ops: list[Any] = []
            if on is not None:
                ops.append(
                    PatchOperation(
                        op="replace",
                        path=f"/environments/{self.environment_key}/on",
                        value=on,
                    )
                )
            if context_targets is not None:
                ops.append(
                    PatchOperation(
                        op="replace",
                        path=f"/environments/{self.environment_key}/contextTargets",
                        value=context_targets,
                    )
                )
            return api.patch_feature_flag(
                project_key=self.project_key,
                feature_flag_key=key,
                patch_with_comment=PatchWithComment(patch=JSONPatch(ops)),
            )

    def delete_flag(self, key: str) -> Any:
        with launchdarkly_api.ApiClient(self.configuration) as api_client:
            api = feature_flags_api.FeatureFlagsApi(api_client)
            return api.delete_feature_flag(self.project_key, key)


def _get_cluster_ip(mz: MaterializeApplication) -> str:
    svc_json = json.loads(mz.kubectl("get", "svc", TOXIPROXY_NAME, "-o", "json"))
    return str(svc_json["spec"]["clusterIP"])


LD_PROXY_CONFIG = {
    "name": LD_PROXY_NAME,
    "listen": "0.0.0.0:443",
    "upstream": f"{LD_HOSTS[0]}:443",
    "enabled": True,
}


def _create_proxy(admin_url: str) -> None:
    requests.post(f"{admin_url}/proxies", json=LD_PROXY_CONFIG).raise_for_status()


def _delete_proxy(admin_url: str) -> None:
    requests.delete(f"{admin_url}/proxies/{LD_PROXY_NAME}")
    r = requests.get(f"{admin_url}/proxies/{LD_PROXY_NAME}")
    assert r.status_code == 404, f"Proxy still exists after delete: {r.text}"


def _add_toxic(
    admin_url: str, name: str, toxic_type: str, attrs: dict[str, Any]
) -> None:
    requests.post(
        f"{admin_url}/proxies/{LD_PROXY_NAME}/toxics",
        json={"name": name, "type": toxic_type, "attributes": attrs},
    ).raise_for_status()


def _remove_toxic(admin_url: str, name: str) -> None:
    requests.delete(f"{admin_url}/proxies/{LD_PROXY_NAME}/toxics/{name}")


def test_launchdarkly_stream_disruption(mz: MaterializeApplication) -> None:
    if not LAUNCHDARKLY_API_TOKEN or not LAUNCHDARKLY_SDK_KEY:
        pytest.skip("LAUNCHDARKLY_API_TOKEN and LAUNCHDARKLY_SDK_KEY required")

    namespace = DEFAULT_K8S_NAMESPACE
    ld = LaunchDarklyClient(LAUNCHDARKLY_API_TOKEN, "default", "ci-cd")
    flag_key = f"ci-test-cloudtest-{BUILDKITE_JOB_ID}"

    toxi_deploy = LdToxiproxyDeployment(namespace)
    toxi_svc = LdToxiproxyService(namespace)

    stateful_sets = [r for r in mz.resources if type(r) == EnvironmentdStatefulSet]
    assert len(stateful_sets) == 1
    original_ss = stateful_sets[0]

    try:
        # --- Deploy toxiproxy ---
        toxi_deploy.create()
        toxi_svc.create()
        wait(condition="condition=Available", resource=f"deployment/{TOXIPROXY_NAME}")

        cluster_ip = _get_cluster_ip(mz)
        admin_port = toxi_svc.node_port("admin")
        admin_url = f"http://localhost:{admin_port}"
        print(f"Toxiproxy ClusterIP={cluster_ip}, admin NodePort={admin_port}")

        # Create the LD proxy (TCP passthrough to stream.launchdarkly.com:443)
        _create_proxy(admin_url)

        # --- Create LD test flag ---
        ld.create_flag(
            flag_key,
            tags=(
                ["ci-test", f"gh-{BUILDKITE_PULL_REQUEST}"]
                if BUILDKITE_PULL_REQUEST
                else ["ci-test"]
            ),
        )
        ld.update_targeting(flag_key, on=True)
        time.sleep(3)

        # --- Configure environmentd: LD sync + hostAliases ---
        ss = copy.deepcopy(original_ss)
        ss.env["MZ_LAUNCHDARKLY_SDK_KEY"] = LAUNCHDARKLY_SDK_KEY
        ss.env["MZ_LAUNCHDARKLY_KEY_MAP"] = f"max_result_size={flag_key}"
        ss.env["MZ_CONFIG_SYNC_LOOP_INTERVAL"] = "1s"

        # Monkey-patch generate_stateful_set so the hostAliases survive
        # replace(), which regenerates the spec internally.
        original_generate = ss.generate_stateful_set
        host_aliases = [V1HostAlias(ip=cluster_ip, hostnames=LD_HOSTS)]

        def patched_generate() -> Any:
            result = original_generate()
            result.spec.template.spec.host_aliases = host_aliases
            return result

        ss.generate_stateful_set = patched_generate  # type: ignore[assignment]
        ss.replace()
        mz.wait_for_sql()

        # --- Verify initial sync (on-variation = 2 GiB) ---
        for _ in range(30):
            result = mz.environmentd.sql_query("SHOW max_result_size")
            if result[0][0] == "2GB":
                break
            time.sleep(1)
        else:
            raise AssertionError(
                f"Expected max_result_size=2GB after LD sync, got {result[0][0]}"
            )
        print("=== Initial LD sync verified (2GB) ===")

        env_id = mz.environmentd.sql_query("SELECT mz_environment_id()")[0][0]

        # First update the flag to 3GB and verify it syncs (proxy still up).
        # This establishes a known good state before disruption.
        ld.update_targeting(
            flag_key,
            context_targets=[
                {"contextKind": "environment", "values": [env_id], "variation": 2}
            ],
        )
        for _ in range(30):
            result = mz.environmentd.sql_query("SHOW max_result_size")
            if result[0][0] == "3GB":
                break
            time.sleep(1)
        else:
            raise AssertionError(
                f"Expected max_result_size=3GB before disruption, got {result[0][0]}"
            )
        print("=== Pre-disruption sync verified (3GB) ===")

        # --- Simulate production failure: reset_peer toxic causes EOF ---
        # This reproduces: "error on event stream: Eof; assuming event stream
        # will reconnect".  The toxic resets connections after 1s, so every
        # reconnection attempt also gets killed — just like the production
        # pattern of repeated Eof/Transport errors.
        print("=== Adding reset_peer toxic (simulates repeated EOF) ===")
        _add_toxic(admin_url, "ld-reset", "reset_peer", {"timeout": 1000})

        ld.update_targeting(
            flag_key,
            context_targets=[
                {"contextKind": "environment", "values": [env_id], "variation": 3}
            ],
        )

        # Let the SDK churn through several reconnect-then-reset cycles
        time.sleep(30)
        result = mz.environmentd.sql_query("SHOW max_result_size")
        assert result[0][0] == "3GB", (
            f"Expected max_result_size to stay at 3GB during reset_peer toxic, "
            f"but got {result[0][0]}"
        )
        print("=== Verified: sync blocked under reset_peer toxic (3GB) ===")

        # --- Remove toxic: SDK must recover (regression gate) ---
        print("=== Removing reset_peer toxic ===")
        _remove_toxic(admin_url, "ld-reset")

        for _ in range(120):
            result = mz.environmentd.sql_query("SHOW max_result_size")
            if result[0][0] == "4294967295B":
                break
            time.sleep(1)
        else:
            raise AssertionError(
                f"Expected max_result_size=4294967295B after removing toxic, "
                f"got {result[0][0]}"
            )
        print("=== Recovery verified: flag sync resumed (4294967295B) ===")

        # --- Second cycle: timeout toxic simulates body read timeout ---
        # This reproduces: "unhandled error on event stream:
        # Transport(TransportError { inner: hyper::Error(Body, Kind(TimedOut)) })"
        print("=== Adding timeout toxic (simulates body read timeout) ===")
        _add_toxic(admin_url, "ld-timeout", "timeout", {"timeout": 3000})

        ld.update_targeting(
            flag_key,
            context_targets=[
                {"contextKind": "environment", "values": [env_id], "variation": 0}
            ],
        )

        time.sleep(30)
        result = mz.environmentd.sql_query("SHOW max_result_size")
        assert result[0][0] == "4294967295B", (
            f"Expected max_result_size to stay at 4294967295B during timeout toxic, "
            f"but got {result[0][0]}"
        )
        print("=== Verified: sync blocked under timeout toxic (4294967295B) ===")

        print("=== Removing timeout toxic ===")
        _remove_toxic(admin_url, "ld-timeout")

        for _ in range(120):
            result = mz.environmentd.sql_query("SHOW max_result_size")
            if result[0][0] == "1GB":
                break
            time.sleep(1)
        else:
            raise AssertionError(
                f"Expected max_result_size=1GB after removing timeout toxic, "
                f"got {result[0][0]}"
            )
        print("=== Recovery verified after timeout toxic (1GB) ===")

    finally:
        try:
            ld.delete_flag(flag_key)
        except Exception:
            pass
        try:
            mz.environmentd.sql(
                "ALTER SYSTEM RESET ALL", port="internal", user="mz_system"
            )
        except Exception:
            pass
        original_ss.replace()
        mz.wait_for_sql()
        try:
            toxi_svc.delete()
        except Exception:
            pass
        try:
            toxi_deploy.delete()
        except Exception:
            pass
