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

Reproduces Transport(TimedOut) and Eof errors seen after the LD SDK 3.0.1
upgrade by using iptables on the Kind node to block traffic to LD endpoints
and killing existing SSE connections via nsenter/ss -K.

The test is a regression gate: after restoring connectivity it asserts that
the LD SDK reconnects and resumes syncing flag values.  If the SDK fails to
recover, the test fails.

Requires LAUNCHDARKLY_SDK_KEY and LAUNCHDARKLY_API_TOKEN env vars.
"""

import copy
import os
import subprocess
import time
from typing import Any
from uuid import uuid1

import launchdarkly_api  # type: ignore
import pytest
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

from materialize.cloudtest import DEFAULT_K8S_CLUSTER_NAME
from materialize.cloudtest.app.materialize_application import MaterializeApplication
from materialize.cloudtest.k8s.environmentd import EnvironmentdStatefulSet

LAUNCHDARKLY_API_TOKEN = os.environ.get("LAUNCHDARKLY_API_TOKEN")
LAUNCHDARKLY_SDK_KEY = os.environ.get("LAUNCHDARKLY_SDK_KEY")
BUILDKITE_JOB_ID = os.environ.get("BUILDKITE_JOB_ID", str(uuid1()))
BUILDKITE_PULL_REQUEST = os.environ.get("BUILDKITE_PULL_REQUEST")

KIND_NODE = f"{DEFAULT_K8S_CLUSTER_NAME}-control-plane"

LD_HOSTS = [
    "stream.launchdarkly.com",
    "sdk.launchdarkly.com",
    "events.launchdarkly.com",
    "app.launchdarkly.com",
]


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


def _resolve_ld_ips() -> list[str]:
    """Resolve LD hostnames inside the Kind node so we get the same IPs
    that the environmentd pod will connect to (CDNs are location-dependent)."""
    ips: set[str] = set()
    for host in LD_HOSTS:
        result = _kind_exec(
            "bash",
            "-c",
            f"getent ahosts {host} | awk '{{print $1}}' | sort -u",
            check=False,
        )
        for line in result.stdout.strip().splitlines():
            ip = line.strip()
            if ip and ":" not in ip:
                ips.add(ip)
    return sorted(ips)


def _kind_exec(*cmd: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["docker", "exec", KIND_NODE, *cmd],
        check=check,
        capture_output=True,
        text=True,
    )


def _kind_iptables(action: str, ips: list[str], check: bool) -> None:
    for ip in ips:
        for table, chain in [("raw", "PREROUTING"), ("raw", "OUTPUT")]:
            _kind_exec(
                "iptables", "-t", table, action, chain,
                "-d", ip, "-j", "DROP",
                check=check,
            )


def _get_envd_pid() -> str:
    """Get the PID of the environmentd container on the Kind node."""
    result = _kind_exec(
        "bash",
        "-c",
        "crictl inspect $(crictl ps --name environmentd -q | head -1) 2>/dev/null | grep '\"pid\"' | head -1 | grep -o '[0-9]*'",
    )
    pid = result.stdout.strip()
    assert pid, "Could not find environmentd container PID"
    return pid


def _kill_connections_in_pod(ips: list[str]) -> None:
    """Kill existing TCP connections inside the environmentd pod's netns."""
    pid = _get_envd_pid()
    for ip in ips:
        _kind_exec(
            "nsenter",
            "--net",
            f"--target={pid}",
            "ss",
            "-K",
            "dst",
            ip,
            check=False,
        )


def _kind_iptables_block(ips: list[str]) -> None:
    """DROP traffic to LD IPs and kill existing connections.
    Uses -I (insert) so rules are evaluated before Docker/Kind ACCEPT rules.
    Then enters the pod's network namespace to kill live SSE streams."""
    _kind_iptables("-I", ips, check=True)
    _kill_connections_in_pod(ips)


def _kind_iptables_unblock(ips: list[str]) -> None:
    _kind_iptables("-D", ips, check=False)


def _get_envd_logs(mz: MaterializeApplication) -> str:
    return mz.kubectl("logs", "environmentd-0", "--tail=2000")


def test_launchdarkly_stream_disruption(mz: MaterializeApplication) -> None:
    if not LAUNCHDARKLY_API_TOKEN or not LAUNCHDARKLY_SDK_KEY:
        pytest.skip("LAUNCHDARKLY_API_TOKEN and LAUNCHDARKLY_SDK_KEY required")

    ld = LaunchDarklyClient(LAUNCHDARKLY_API_TOKEN, "default", "ci-cd")
    flag_key = f"ci-test-cloudtest-{BUILDKITE_JOB_ID}"

    ld_ips = _resolve_ld_ips()
    assert len(ld_ips) > 0, "Failed to resolve any LaunchDarkly IP addresses"
    print(f"Resolved LD IPs: {ld_ips}")

    try:
        # Create and enable a test flag
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

        # Configure environmentd with LD sync
        stateful_set = [r for r in mz.resources if type(r) == EnvironmentdStatefulSet]
        assert len(stateful_set) == 1
        original_ss = stateful_set[0]
        ss = copy.deepcopy(original_ss)
        ss.env["MZ_LAUNCHDARKLY_SDK_KEY"] = LAUNCHDARKLY_SDK_KEY
        ss.env["MZ_LAUNCHDARKLY_KEY_MAP"] = f"max_result_size={flag_key}"
        ss.env["MZ_CONFIG_SYNC_LOOP_INTERVAL"] = "1s"
        ss.replace()
        mz.wait_for_sql()

        # Verify initial LD sync works (on-variation = 2 GiB)
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

        # --- Block LD traffic and verify sync stops ---
        print("=== Blocking LD traffic on Kind node ===")
        _kind_iptables_block(ld_ips)

        # Change the flag while blocked — the new value must NOT propagate.
        ld.update_targeting(
            flag_key,
            context_targets=[
                {
                    "contextKind": "environment",
                    "values": [env_id],
                    "variation": 2,
                }
            ],
        )

        # Wait and verify the value stays at 2GB (sync is broken).
        time.sleep(20)
        result = mz.environmentd.sql_query("SHOW max_result_size")
        assert result[0][0] == "2GB", (
            f"Expected max_result_size to stay at 2GB while blocked, "
            f"but got {result[0][0]} — LD block is ineffective"
        )
        print("=== Verified: sync blocked (value stayed at 2GB) ===")

        # --- Unblock and verify recovery ---
        print("=== Unblocking LD traffic ===")
        _kind_iptables_unblock(ld_ips)

        for _ in range(120):
            result = mz.environmentd.sql_query("SHOW max_result_size")
            if result[0][0] == "3GB":
                break
            time.sleep(1)
        else:
            raise AssertionError(
                f"Expected max_result_size=3GB after unblocking, got {result[0][0]}"
            )
        print("=== Recovery verified: flag sync resumed (3GB) ===")

        # --- Second disruption/recovery cycle ---
        print("=== Second disruption cycle ===")
        _kind_iptables_block(ld_ips)

        ld.update_targeting(
            flag_key,
            context_targets=[
                {
                    "contextKind": "environment",
                    "values": [env_id],
                    "variation": 3,
                }
            ],
        )

        time.sleep(20)
        result = mz.environmentd.sql_query("SHOW max_result_size")
        assert result[0][0] == "3GB", (
            f"Expected max_result_size to stay at 3GB while blocked, "
            f"but got {result[0][0]}"
        )
        print("=== Verified: sync blocked again (value stayed at 3GB) ===")

        _kind_iptables_unblock(ld_ips)

        for _ in range(120):
            result = mz.environmentd.sql_query("SHOW max_result_size")
            if result[0][0] == "4294967295B":
                break
            time.sleep(1)
        else:
            raise AssertionError(
                f"Expected max_result_size=4294967295B after 2nd recovery, "
                f"got {result[0][0]}"
            )
        print("=== Second recovery verified (4294967295B) ===")

        # Print any LD-related errors for debugging
        logs = _get_envd_logs(mz)
        print("\n=== Final LD-related log lines ===")
        for line in logs.splitlines():
            if any(
                kw in line
                for kw in [
                    "error on event stream",
                    "unhandled error",
                    "data_source",
                    "SystemParameterFrontend",
                ]
            ):
                print(f"  {line}")

    finally:
        _kind_iptables_unblock(ld_ips)
        try:
            ld.delete_flag(flag_key)
        except Exception:
            pass
        # Reset all system parameters to defaults before restoring the
        # original StatefulSet.  LD sync persists values in the catalog
        # and they survive the environmentd restart, which would pollute
        # subsequent tests.
        try:
            mz.environmentd.sql(
                "ALTER SYSTEM RESET ALL",
                port="internal",
                user="mz_system",
            )
        except Exception:
            pass
        original_ss.replace()
        mz.wait_for_sql()
