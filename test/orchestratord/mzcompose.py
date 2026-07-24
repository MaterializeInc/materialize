# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Test `orchestratord`
"""

import argparse
import copy
import datetime
import json
import os
import random
import shutil
import signal
import subprocess
import tempfile
import time
import uuid
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from enum import Enum
from typing import Any

import psycopg
import requests
import yaml
from psycopg import sql as psycopg_sql
from semver.version import Version

from materialize import MZ_ROOT, buildkite, ci_util, git, spawn
from materialize.mz_version import MzVersion
from materialize.mzcompose.composition import (
    Composition,
    Service,
    WorkflowArgumentParser,
)
from materialize.mzcompose.services.balancerd import Balancerd
from materialize.mzcompose.services.clusterd import Clusterd
from materialize.mzcompose.services.environmentd import Environmentd
from materialize.mzcompose.services.mz import Mz
from materialize.mzcompose.services.mz_debug import MzDebug
from materialize.mzcompose.services.orchestratord import Orchestratord
from materialize.mzcompose.services.testdrive import Testdrive
from materialize.test_analytics.config.test_analytics_db_config import (
    create_test_analytics_config,
)
from materialize.test_analytics.data.upgrade_downtime import (
    upgrade_downtime_result_storage,
)
from materialize.test_analytics.test_analytics_db import TestAnalyticsDb
from materialize.util import PropagatingThread, all_subclasses
from materialize.version_list import (
    get_all_self_managed_versions,
    get_self_managed_versions,
)

SERVICES = [
    Testdrive(),
    Orchestratord(),
    Environmentd(),
    Clusterd(),
    Balancerd(),
    MzDebug(),
    Mz(app_password=""),
]

KIND_CLUSTER_NAME = "kind"


def run_mz_debug() -> None:
    # TODO: Hangs a lot in CI
    # Only using capture because it's too noisy
    # spawn.capture(
    #     [
    #         "./mz-debug",
    #         "self-managed",
    #         "--k8s-namespace",
    #         "materialize-environment",
    #         "--mz-instance-name",
    #         "12345678-1234-1234-1234-123456789012",
    #     ]
    # )
    pass


def get_tag(tag: str | None = None) -> str:
    # We can't use the mzbuild tag because it has a different fingerprint for
    # environmentd/clusterd/balancerd and the orchestratord depends on them
    # being identical.
    return tag or f"v{ci_util.get_mz_version()}--pr.g{git.rev_parse('HEAD')}"


def get_version(tag: str | None = None) -> MzVersion:
    return MzVersion.parse_mz(get_tag(tag))


def get_image(image: str, tag: str | None) -> str:
    return f"{image.rsplit(':', 1)[0]}:{get_tag(tag)}"


def get_upgrade_target(
    rng: random.Random, current_version: MzVersion, versions: list[MzVersion]
) -> MzVersion:
    for version in rng.sample(versions, k=len(versions)):
        if version <= current_version:
            continue
        if (
            current_version.major == 0
            and current_version.minor == 130
            and version.major == 0
            and version.minor == 147
        ):
            return version
        if (
            current_version.major == 0
            and current_version.minor == 147
            and current_version.patch < 20
            and version.major == 0
            and version.minor == 147
        ):
            return version
        if (
            current_version.major == 0
            and current_version.minor == 147
            and current_version.patch >= 20
            and version.major == 26
            and version.minor == 0
        ):
            return version
        if current_version.major >= 26 and current_version.major + 1 >= version.major:
            return version
    raise ValueError(
        f"No potential upgrade target for {current_version} found in {versions}"
    )


def get_pod_data(
    labels: dict[str, str], namespace="materialize-environment"
) -> dict[str, Any]:
    return json.loads(
        spawn.capture(
            [
                "kubectl",
                "get",
                "pod",
                "-l",
                ",".join(f"{key}={value}" for key, value in labels.items()),
                "-n",
                namespace,
                "-o",
                "json",
            ]
        )
    )


def get_orchestratord_data() -> dict[str, Any]:
    return get_pod_data(
        labels={"app.kubernetes.io/instance": "operator"},
        namespace="materialize",
    )


def get_balancerd_data() -> dict[str, Any]:
    return get_pod_data(
        labels={"materialize.cloud/app": "balancerd"},
    )


def get_console_data() -> dict[str, Any]:
    return get_pod_data(
        labels={"materialize.cloud/app": "console"},
    )


def get_environmentd_data() -> dict[str, Any]:
    return get_pod_data(
        labels={"materialize.cloud/app": "environmentd"},
    )


def get_clusterd_data() -> dict[str, Any]:
    return get_pod_data(
        labels={"environmentd.materialize.cloud/namespace": "cluster"},
    )


def get_console_app_config(namespace="materialize-environment") -> dict[str, Any]:
    """Return the parsed contents of the console's `app-config.json` configmap."""
    data = json.loads(
        spawn.capture(
            [
                "kubectl",
                "get",
                "configmap",
                "-l",
                "materialize.cloud/app=console",
                "-n",
                namespace,
                "-o",
                "json",
            ]
        )
    )
    items = data["items"]
    assert len(items) == 1, f"Expected exactly one console configmap, but got {items}"
    return json.loads(items[0]["data"]["app-config.json"])


def ensure_kind_version() -> None:
    kind_version = Version.parse(spawn.capture(["kind", "version"]).split(" ")[1][1:])
    assert kind_version >= Version.parse(
        "0.29.0"
    ), f"kind >= v0.29.0 required, while you are on {kind_version}"


def stop_and_remove_container(container_name: str) -> None:
    try:
        spawn.runv(["docker", "stop", container_name])
    except:
        pass
    try:
        spawn.runv(["docker", "rm", container_name])
    except:
        pass


def start_registry_proxy(
    *,
    container_name: str,
    cache_dir: str,
    remote_url: str,
    username: str | None = None,
    password: str | None = None,
) -> None:
    env = ["-e", f"REGISTRY_PROXY_REMOTEURL={remote_url}"]
    if username and password:
        env.extend(
            [
                "-e",
                f"REGISTRY_PROXY_USERNAME={username}",
                "-e",
                f"REGISTRY_PROXY_PASSWORD={password}",
            ]
        )

    spawn.runv(
        [
            "docker",
            "run",
            "-d",
            "--name",
            container_name,
            "--restart=always",
            "--net=kind",
            "-v",
            cache_dir,
            *env,
            "registry:2",
        ]
    )


def render_kind_cluster_config() -> None:
    with (
        open(MZ_ROOT / "test" / "orchestratord" / "cluster.yaml.tmpl") as in_file,
        open(MZ_ROOT / "test" / "orchestratord" / "cluster.yaml", "w") as out_file,
    ):
        text = in_file.read()
        out_file.write(
            text.replace(
                "$DOCKER_CONFIG",
                os.getenv("DOCKER_CONFIG", f"{os.environ['HOME']}/.docker"),
            )
        )


def install_metrics_server() -> None:
    spawn.runv(
        [
            "helm",
            "repo",
            "add",
            "metrics-server",
            "https://kubernetes-sigs.github.io/metrics-server/",
        ]
    )
    spawn.runv(["helm", "repo", "update", "metrics-server"])
    spawn.runv(
        [
            "helm",
            "install",
            "metrics-server",
            "metrics-server/metrics-server",
            "--namespace",
            "kube-system",
            "--set",
            "args={--kubelet-insecure-tls,--kubelet-preferred-address-types=InternalIP,Hostname,ExternalIP}",
        ]
    )


def recreate_kind_cluster() -> None:
    ensure_kind_version()

    spawn.runv(["kind", "delete", "cluster", "--name", KIND_CLUSTER_NAME])

    try:
        spawn.runv(["docker", "network", "create", "kind"])
    except:
        pass

    stop_and_remove_container("proxy-dockerhub")
    stop_and_remove_container("proxy-ghcr")

    start_registry_proxy(
        container_name="proxy-dockerhub",
        cache_dir=f"{MZ_ROOT}/misc/kind/cache/dockerhub:/var/lib/registry",
        remote_url="https://registry-1.docker.io",
        username=os.getenv("DOCKERHUB_USERNAME"),
        password=os.getenv("DOCKERHUB_ACCESS_TOKEN"),
    )
    start_registry_proxy(
        container_name="proxy-ghcr",
        cache_dir=f"{MZ_ROOT}/misc/kind/cache/ghcr:/var/lib/registry",
        remote_url="https://ghcr.io",
        username="materialize-bot",
        password=os.getenv("GITHUB_GHCR_TOKEN"),
    )

    render_kind_cluster_config()
    spawn.runv(
        [
            "kind",
            "create",
            "cluster",
            "--name",
            KIND_CLUSTER_NAME,
            "--config",
            MZ_ROOT / "test" / "orchestratord" / "cluster.yaml",
        ]
    )

    retry(install_metrics_server, 20)


@contextmanager
def port_forward_environmentd(
    port: int = 6875,
    namespace: str = "materialize-environment",
) -> Iterator[int]:
    """
    Context manager that sets up port forwarding to environmentd.

    Usage:
        with port_forward_environmentd() as port:
            conn = psycopg.connect(f"host=localhost port={port} user=materialize")
            # ... use connection ...
    """
    environmentd = get_environmentd_data()
    pod_name = environmentd["items"][0]["metadata"]["name"]

    process = subprocess.Popen(
        [
            "kubectl",
            "port-forward",
            f"pod/{pod_name}",
            f"{port}:{port}",
            "-n",
            namespace,
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    try:
        # Wait briefly for port-forward to establish
        time.sleep(2)

        # Check if process is still running
        if process.poll() is not None:
            stdout, stderr = process.communicate()
            raise RuntimeError(
                f"Port forward failed to start: stdout={stdout.decode()}, stderr={stderr.decode()}"
            )

        yield port
    finally:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()


def retry(fn: Callable, timeout: int) -> None:
    end_time = (
        datetime.datetime.now() + datetime.timedelta(seconds=timeout)
    ).timestamp()
    while time.time() < end_time:
        try:
            fn()
            return
        except:
            pass
        time.sleep(1)
    fn()


def download_repo_file_at_tag(path: str, tag: str) -> bytes:
    """Fetch a repository file at a git tag from GitHub.

    Uses the authenticated Contents API when GITHUB_TOKEN is set (5000
    requests/hour), which is what CI relies on. Falls back to anonymous
    raw.githubusercontent.com otherwise. Anonymous raw content throttles
    shared CI IPs with 429, so retry with backoff on rate-limit and 5xx
    statuses, honoring Retry-After when present.
    """
    token = os.getenv("GITHUB_CI_ISSUE_REFERENCE_CHECKER_TOKEN") or os.getenv(
        "GITHUB_TOKEN"
    )
    if token:
        url = f"https://api.github.com/repos/MaterializeInc/materialize/contents/{path}?ref={tag}"
        headers = {
            "Accept": "application/vnd.github.raw",
            "Authorization": f"Bearer {token}",
            "X-GitHub-Api-Version": "2022-11-28",
        }
    else:
        url = f"https://raw.githubusercontent.com/MaterializeInc/materialize/refs/tags/{tag}/{path}"
        headers = {}

    delay = 2.0
    for _ in range(8):
        response = requests.get(url, headers=headers, timeout=30)
        if response.status_code == 200:
            return response.content
        if response.status_code not in (429, 500, 502, 503, 504):
            break
        wait = float(response.headers.get("Retry-After", delay))
        print(f"Got {response.status_code} for {url}, retrying in {wait}s")
        time.sleep(wait)
        delay = min(delay * 2, 60)
    raise AssertionError(
        f"Failed to download {path} at {tag} from {url}: {response.status_code}"
    )


# TODO: Cover src/cloud-resources/src/crd/materialize.rs
# TODO: Cover https://materialize.com/docs/installation/configuration/


class Modification:
    pick_by_default: bool = True

    def __init__(self, value: Any):
        assert value in self.values(
            get_version()
        ), f"Expected {value} to be in {self.values(get_version())}"
        self.value = value

    def to_dict(self) -> dict[str, Any]:
        return {"modification": self.__class__.__name__, "value": self.value}

    def __eq__(self, other: object):
        if not isinstance(other, self.__class__):
            return NotImplemented
        return self.value == other.value

    def __ne__(self, other):
        return not self.__eq__(other)

    @classmethod
    def default(cls) -> Any:
        raise NotImplementedError

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Modification":
        name = data["modification"]
        for subclass in all_subclasses(Modification):
            if subclass.__name__ == name:
                break
        else:
            raise ValueError(
                f"No modification with name {name} found, only know {[subclass.__name__ for subclass in all_modifications()]}"
            )
        return subclass(data["value"])

    @classmethod
    def values(cls, version: MzVersion) -> list[Any]:
        raise NotImplementedError

    @classmethod
    def bad_values(cls) -> list[Any]:
        return cls.failed_reconciliation_values()

    @classmethod
    def good_values(cls, version: MzVersion) -> list[Any]:
        return [value for value in cls.values(version) if value not in cls.bad_values()]

    @classmethod
    def failed_reconciliation_values(cls) -> list[Any]:
        return []

    def modify(self, definition: dict[str, Any]) -> None:
        raise NotImplementedError

    def validate(self, mods: dict[type["Modification"], Any]) -> None:
        raise NotImplementedError


def all_modifications() -> list[type[Modification]]:
    return [
        mod_class
        for mod_class in all_subclasses(Modification)
        if mod_class.pick_by_default
    ]


class LicenseKey(Modification):
    @classmethod
    def values(cls, version: MzVersion) -> list[Any]:
        # TODO: Reenable "del" when database-issues#9928 is fixed
        # return ["valid", "invalid", "del"]
        return ["valid", "invalid"]

    @classmethod
    def failed_reconciliation_values(cls) -> list[Any]:
        return ["invalid", "del"]

    @classmethod
    def default(cls) -> Any:
        return "valid"

    def modify(self, definition: dict[str, Any]) -> None:
        if self.value == "valid":
            definition["secret"]["stringData"]["license_key"] = os.environ[
                "MZ_CI_LICENSE_KEY"
            ]
        elif self.value == "invalid":
            definition["secret"]["stringData"]["license_key"] = "foobar"
        elif self.value == "del":
            del definition["secret"]["stringData"]["license_key"]
        else:
            raise ValueError(
                f"Unknown value {self.value}, only know {self.values(get_version())}"
            )

    def validate(self, mods: dict[type[Modification], Any]) -> None:
        if MzVersion.parse_mz(mods[EnvironmentdImageRef]) < MzVersion.parse_mz(
            "v0.147.0"
        ):
            return

        def check() -> None:
            environmentd = get_environmentd_data()
            envs = environmentd["items"][0]["spec"]["containers"][0]["env"]
            for env in envs:
                if env["name"] != "MZ_LICENSE_KEY":
                    continue
                expected = "/license_key/license_key"
                assert (
                    env["value"] == expected
                ), f"Expected license key to be set to {expected}, but is {env['value']}"
                break
            else:
                assert (
                    False
                ), f"Expected to find MZ_LICENSE_KEY in env variables, but only found {envs}"
            ready = environmentd["items"][0]["status"]["containerStatuses"][0]["ready"]
            assert ready, "Expected environmentd to be in ready state"

        retry(check, 240)


class LicenseKeyCheck(Modification):
    @classmethod
    def values(cls, version: MzVersion) -> list[Any]:
        # TODO: Reenable False when fixed
        return [None, True]

    @classmethod
    def default(cls) -> Any:
        return None

    def modify(self, definition: dict[str, Any]) -> None:
        if self.value is not None:
            definition["operator"]["operator"]["args"]["enableLicenseKeyChecks"] = bool(
                self.value
            )

    def validate(self, mods: dict[type[Modification], Any]) -> None:
        environmentd = get_environmentd_data()
        args = environmentd["items"][0]["spec"]["containers"][0]["args"]
        expected = "--disable-license-key-checks"
        if self.value is None or self.value:
            assert (
                expected not in args
            ), f"Expected no {expected} in environmentd args, but found it: {args}"
        else:
            assert (
                expected in args
            ), f"Expected {expected} in environmentd args, but only found {args}"


class BalancerdEnabled(Modification):
    @classmethod
    def values(cls, version: MzVersion) -> list[Any]:
        return [True, False]

    @classmethod
    def default(cls) -> Any:
        return True

    def modify(self, definition: dict[str, Any]) -> None:
        definition["operator"]["balancerd"]["enabled"] = self.value

    def validate(self, mods: dict[type[Modification], Any]) -> None:
        if MzVersion.parse_mz(mods[EnvironmentdImageRef]) < MzVersion.parse_mz(
            "v0.148.0"
        ):
            return

        def check() -> None:
            result = spawn.capture(
                [
                    "kubectl",
                    "get",
                    "pods",
                    "-l",
                    "app=balancerd",
                    "-n",
                    "materialize-environment",
                    "-o",
                    "name",
                ],
            )
            if self.value:
                assert result, f"Unexpected result: {result}"
            else:
                assert not result, f"Unexpected result: {result}"

        # Balancerd can take a while to start up
        retry(check, 240)


class BalancerdNodeSelector(Modification):
    @classmethod
    def values(cls, version: MzVersion) -> list[Any]:
        return [
            {},
            # TODO: Reenable when we create nodes with these labels
            # {"materialize.cloud/foo": "bar"},
            # {"materialize.cloud/a": "b", "materialize.cloud/cd": "ef"},
        ]

    @classmethod
    def default(cls) -> Any:
        return {}

    def modify(self, definition: dict[str, Any]) -> None:
        definition["operator"]["balancerd"]["nodeSelector"] = self.value

    def validate(self, mods: dict[type[Modification], Any]) -> None:
        if MzVersion.parse_mz(mods[EnvironmentdImageRef]) < MzVersion.parse_mz(
            "v0.148.0"
        ):
            return

        def check() -> None:
            balancerd = get_balancerd_data()
            if self.value and mods[BalancerdEnabled]:
                nodeSelector = balancerd["items"][0]["spec"].get("nodeSelector")
                assert (
                    nodeSelector == self.value
                ), f"Expected nodeSelectors {self.value} but got {nodeSelector}"
            elif mods[BalancerdEnabled]:
                nodeSelector = balancerd["items"][0]["spec"].get("nodeSelector")
                assert not nodeSelector, f"Unexpected nodeSelectors: {nodeSelector}"
            else:
                assert not balancerd["items"], f"Unexpected items: {balancerd['items']}"

        # Balancerd can take a while to start up
        retry(check, 240)


class ConsoleEnabled(Modification):
    @classmethod
    def values(cls, version: MzVersion) -> list[Any]:
        return [True, False]

    @classmethod
    def default(cls) -> Any:
        return True

    def modify(self, definition: dict[str, Any]) -> None:
        definition["operator"]["console"]["enabled"] = self.value

    def validate(self, mods: dict[type[Modification], Any]) -> None:
        # TODO: Should this work with older versions? Fails in upgrade chain: AssertionError: Unexpected result: pod/mz9bvcfyoxae-console-654bd7f8f5-fbv4q
        if MzVersion.parse_mz(mods[EnvironmentdImageRef]) < MzVersion.parse_mz(
            "v0.148.0"
        ):
            return

        def check() -> None:
            pass  # TODO: https://linear.app/materializeinc/issue/DB-105
            # result = spawn.capture(
            #     [
            #         "kubectl",
            #         "get",
            #         "pods",
            #         "-l",
            #         "app=console",
            #         "-n",
            #         "materialize-environment",
            #         "-o",
            #         "name",
            #     ],
            # )
            # if self.value:
            #     assert result, f"Unexpected result: {result}"
            # else:
            #     assert not result, f"Unexpected result: {result}"

        # Console can take a while to start up
        retry(check, 120)


class EnableRBAC(Modification):
    @classmethod
    def values(cls, version: MzVersion) -> list[Any]:
        return [True, False]

    @classmethod
    def default(cls) -> Any:
        return False

    def modify(self, definition: dict[str, Any]) -> None:
        definition["materialize"]["spec"]["enableRbac"] = self.value

    def validate(self, mods: dict[type[Modification], Any]) -> None:
        environmentd = get_environmentd_data()
        args = environmentd["items"][0]["spec"]["containers"][0]["args"]
        expected = "--system-parameter-default=enable_rbac_checks=false"
        if self.value:
            assert (
                expected not in args
            ), f"Expected no {expected} in environmentd args, but found it: {args}"
        else:
            assert (
                expected in args
            ), f"Expected {expected} in environmentd args, but only found {args}"


class EnvironmentdImageRef(Modification):
    # Only done intentionally during upgrades with correct ordering
    pick_by_default = False

    @classmethod
    def values(cls, version: MzVersion) -> list[Any]:
        return [str(version) for version in get_all_self_managed_versions()] + [
            get_tag()
        ]

    @classmethod
    def default(cls) -> Any:
        return get_tag()

    def __init__(self, value: Any):
        self.value = value

    def modify(self, definition: dict[str, Any]) -> None:
        definition["materialize"]["spec"][
            "environmentdImageRef"
        ] = f"materialize/environmentd:{self.value}"

    def validate(self, mods: dict[type[Modification], Any]) -> None:
        def check() -> None:
            environmentd = get_environmentd_data()
            for item in environmentd["items"]:
                # Skip terminating pods from a previous rolling update
                if item.get("metadata", {}).get("deletionTimestamp"):
                    continue
                image = item["spec"]["containers"][0]["image"]
                expected = f"materialize/environmentd:{self.value}"
                assert (
                    image == expected or f"ghcr.io/materializeinc/{image}" == expected
                ), f"Expected environmentd image {expected}, but found {image}"

            balancerd = get_balancerd_data()
            for item in balancerd["items"]:
                # Skip terminating pods from a previous rolling update
                if item.get("metadata", {}).get("deletionTimestamp"):
                    continue
                image = item["spec"]["containers"][0]["image"]
                expected = f"materialize/balancerd:{self.value}"
                assert (
                    image == expected or f"ghcr.io/materializeinc/{image}" == expected
                ), f"Expected balancerd image {expected}, but found {image}"

            # TODO: Console version is currently set via --console-image-tag-default
            # console = get_console_data()
            # for item in console["items"]:
            #     image = item["spec"]["containers"][0]["image"]
            #     expected = f"materialize/console:{self.value}"
            #     assert (
            #         image == expected or f"ghcr.io/materializeinc/{image}" == expected
            #     ), f"Expected console image {expected}, but found {image}"

        retry(check, 240)


class NumMaterializeEnvironments(Modification):
    # Only done intentionally
    pick_by_default = False

    @classmethod
    def values(cls, version: MzVersion) -> list[Any]:
        return [1, 2]

    @classmethod
    def default(cls) -> Any:
        return 1

    def modify(self, definition: dict[str, Any]) -> None:
        if self.value == 2:
            definition["materialize2"] = copy.deepcopy(definition["materialize"])
            definition["materialize2"]["metadata"][
                "name"
            ] = "12345678-1234-1234-1234-123456789013"
            # TODO: Also need a different pg db?
        elif self.value == 1:
            if "materialize2" in definition:
                del definition["materialize2"]
        else:
            raise ValueError(f"Unhandled value {self.value}")

    def validate(self, mods: dict[type[Modification], Any]) -> None:
        service_names = (
            spawn.capture(
                [
                    "kubectl",
                    "get",
                    "services",
                    "-n",
                    "materialize-environment",
                    "-o",
                    "name",
                ],
                stderr=subprocess.DEVNULL,
            )
            .strip()
            .split("\n")
        )

        def check() -> None:
            for service_name in service_names:
                if not "-cluster-" in service_name:
                    continue
                data = json.loads(
                    spawn.capture(
                        [
                            "kubectl",
                            "get",
                            "endpoints",
                            service_name.removeprefix("service/"),
                            "-n",
                            "materialize-environment",
                            "-o",
                            "json",
                        ]
                    )
                )
                print(data)
                addresses = data["subsets"][0]["addresses"]
                assert (
                    len(addresses) == 1
                ), f"Expected 1 address for clusterd, but found {addresses}"

        retry(check, 120)


class TelemetryEnabled(Modification):
    @classmethod
    def values(cls, version: MzVersion) -> list[Any]:
        return [True, False]

    @classmethod
    def default(cls) -> Any:
        return True

    def modify(self, definition: dict[str, Any]) -> None:
        definition["operator"]["telemetry"]["enabled"] = self.value

    def validate(self, mods: dict[type[Modification], Any]) -> None:
        environmentd = get_environmentd_data()
        args = environmentd["items"][0]["spec"]["containers"][0]["args"]
        expected = "--segment-api-key="
        if self.value:
            assert any(
                expected in arg for arg in args
            ), f"Expected {expected} in environmentd args, but only found: {args}"
        else:
            assert not any(
                expected in arg for arg in args
            ), f"Expected no {expected} in environmentd args, but found {args}"


class TelemetrySegmentClientSide(Modification):
    @classmethod
    def values(cls, version: MzVersion) -> list[Any]:
        return [True, False]

    @classmethod
    def default(cls) -> Any:
        return True

    def modify(self, definition: dict[str, Any]) -> None:
        definition["operator"]["telemetry"]["segmentClientSide"] = self.value

    def validate(self, mods: dict[type[Modification], Any]) -> None:
        environmentd = get_environmentd_data()
        args = environmentd["items"][0]["spec"]["containers"][0]["args"]
        expected = "--segment-client-side"
        if self.value and mods[TelemetryEnabled]:
            assert (
                expected in args
            ), f"Expected {expected} in environmentd args, but only found: {args}"
        else:
            assert (
                not expected in args
            ), f"Expected no {expected} in environmentd args, but found {args}"


class ObservabilityEnabled(Modification):
    @classmethod
    def values(cls, version: MzVersion) -> list[Any]:
        return [True, False]

    @classmethod
    def default(cls) -> Any:
        return True

    def modify(self, definition: dict[str, Any]) -> None:
        definition["operator"]["observability"]["enabled"] = self.value

    def validate(self, mods: dict[type[Modification], Any]) -> None:
        pass  # Has no effect on its own


class ObservabilityPodMetricsEnabled(Modification):
    @classmethod
    def values(cls, version: MzVersion) -> list[Any]:
        return [True, False]

    @classmethod
    def default(cls) -> Any:
        return False

    def modify(self, definition: dict[str, Any]) -> None:
        definition["operator"]["observability"]["podMetrics"]["enabled"] = self.value

    def validate(self, mods: dict[type[Modification], Any]) -> None:
        return  # TODO: Doesn't work with upgrade: Expected no --collect-pod-metrics in environmentd args, but found it


class ObservabilityPrometheusScrapeAnnotationsEnabled(Modification):
    @classmethod
    def values(cls, version: MzVersion) -> list[Any]:
        return [True, False]

    @classmethod
    def default(cls) -> Any:
        return True

    def modify(self, definition: dict[str, Any]) -> None:
        definition["operator"]["observability"]["prometheus"]["scrapeAnnotations"][
            "enabled"
        ] = self.value

    def validate(self, mods: dict[type[Modification], Any]) -> None:
        orchestratord = get_orchestratord_data()
        args = orchestratord["items"][0]["spec"]["containers"][0]["args"]
        expected = "--enable-prometheus-scrape-annotations"
        if self.value and mods[ObservabilityEnabled]:
            assert (
                expected in args
            ), f"Expected {expected} in environmentd args, but only found {args}"
        else:
            assert (
                expected not in args
            ), f"Expected no {expected} in environmentd args, but found it: {args}"


# TODO: Fix in upgrade tests
# class BalancerdReplicas(Modification):
#     @classmethod
#     def values(cls, version: MzVersion) -> list[Any]:
#         return [None, 1, 2]
#
#     @classmethod
#     def default(cls) -> Any:
#         return None
#
#     def modify(self, definition: dict[str, Any]) -> None:
#         if self.value is not None:
#             definition["materialize"]["spec"]["balancerdReplicas"] = self.value
#
#     def validate(self, mods: dict[type[Modification], Any]) -> None:
#         if not mods[BalancerdEnabled]:
#             return
#
#         def check_replicas():
#             balancerd = get_balancerd_data()
#             num_pods = len(balancerd["items"])
#             expected = self.value if self.value is not None else 2
#             assert (
#                 num_pods == expected
#             ), f"Expected {expected} balancerd pods, but found {num_pods}"
#
#         retry(check_replicas, 120)


class ConsoleReplicas(Modification):
    @classmethod
    def values(cls, version: MzVersion) -> list[Any]:
        return [None, 1, 2]

    @classmethod
    def default(cls) -> Any:
        return None

    def modify(self, definition: dict[str, Any]) -> None:
        if self.value is not None:
            definition["materialize"]["spec"]["consoleReplicas"] = self.value

    def validate(self, mods: dict[type[Modification], Any]) -> None:
        if not mods[ConsoleEnabled]:
            return

        def check_replicas():
            console = get_console_data()
            len(console["items"])
            # TODO: https://linear.app/materializeinc/issue/DB-105
            # assert (
            #     num_pods == expected
            # ), f"Expected {expected} console pods, but found {num_pods}"

        # console doesn't get launched until last
        retry(check_replicas, 120)


class SystemParamConfigMap(Modification):
    @classmethod
    def values(cls, version: MzVersion) -> list[Any]:
        result: list[Any] = [None]
        if version >= MzVersion.parse_mz("v26.1.0"):
            result.append({"max_connections": 1000})
        return result

    @classmethod
    def default(cls) -> Any:
        return None

    def modify(self, definition: dict[str, Any]) -> None:
        if self.value is not None:
            # Create a configmap with system parameters
            configmap_name = "system-params-test"

            # First create the configmap in the cluster
            configmap = {
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {
                    "name": configmap_name,
                    "namespace": "materialize-environment",
                },
                "data": {
                    "system-params.json": json.dumps(self.value),
                },
            }
            # Apply the configmap (will be done in init/run)
            definition["system_params_configmap"] = configmap
            # Set the configmap name in the Materialize spec
            definition["materialize"]["spec"][
                "systemParameterConfigmapName"
            ] = configmap_name

    def validate(self, mods: dict[type[Modification], Any]) -> None:
        if self.value is None:
            return

        def check() -> None:
            environmentd = get_environmentd_data()

            # Check that the volume is mounted
            volumes = environmentd["items"][0]["spec"]["volumes"]
            volume_found = False
            for volume in volumes:
                if volume.get("name") == "system-params":
                    assert (
                        volume.get("configMap") is not None
                    ), f"Expected configMap in volume, but found {volume}"
                    assert (
                        volume["configMap"]["name"] == "system-params-test"
                    ), f"Expected configmap name system-params-test, but found {volume['configMap']['name']}"
                    volume_found = True
                    break
            assert volume_found, f"Expected to find system-params volume in {volumes}"

            # Check that the volume mount is present
            volume_mounts = environmentd["items"][0]["spec"]["containers"][0][
                "volumeMounts"
            ]
            volume_mount_found = False
            for mount in volume_mounts:
                if mount.get("name") == "system-params":
                    assert (
                        mount.get("mountPath") == "/system-params"
                    ), f"Expected mount path /system-params, but found {mount['mountPath']}"
                    volume_mount_found = True
                    break
            assert (
                volume_mount_found
            ), f"Expected to find system-params volume mount in {volume_mounts}"

            # Check that the config-sync-file-path arg is present
            args = environmentd["items"][0]["spec"]["containers"][0]["args"]
            expected_arg = "--config-sync-file-path=/system-params/system-params.json"
            assert any(
                expected_arg in arg for arg in args
            ), f"Expected {expected_arg} in environmentd args, but only found: {args}"

        retry(check, 240)

        version = MzVersion.parse_mz(mods[EnvironmentdImageRef])
        auth = mods[AuthenticatorKind]
        port = (
            6875
            if (version >= MzVersion.parse_mz("v0.147.0") and auth == "Password")
            or (version >= MzVersion.parse_mz("v26.0.0") and auth == "Sasl")
            or (version >= MzVersion.parse_mz("v26.16.0-dev.0") and auth == "Oidc")
            else 6877
        )

        # Validate that the system parameters have actually been applied via SQL
        def check_system_params() -> None:
            with port_forward_environmentd(port) as used_port:
                with psycopg.connect(
                    f"host=localhost port={used_port} user=mz_system password=superpassword sslmode=disable"
                ) as conn:
                    with conn.cursor() as cur:
                        for param_name, expected_value in self.value.items():
                            cur.execute(
                                psycopg_sql.SQL("SHOW {}").format(
                                    psycopg_sql.Identifier(param_name)
                                )
                            )
                            result = cur.fetchone()
                            assert (
                                result is not None
                            ), f"No result for SHOW {param_name}"
                            actual_value = result[0]
                            assert str(actual_value) == str(
                                expected_value
                            ), f"Expected {param_name}={expected_value}, but got {actual_value}"

        retry(check_system_params, 240)


def validate_cluster_replica_size(
    size: dict[str, Any], swap_enabled: bool, storage_class_name_set: bool
):
    assert isinstance(size["is_cc"], bool)
    assert size["is_cc"]

    if swap_enabled:
        assert size["disk_limit"] == "0"
        assert isinstance(size["swap_enabled"], bool)
        assert size["swap_enabled"]
    elif storage_class_name_set:
        assert size["disk_limit"] != "0"
        assert not size["swap_enabled"]
    else:
        assert size["disk_limit"] == "0"

    validate_node_selector(size["selectors"], swap_enabled, storage_class_name_set)


def validate_node_selector(
    selector: dict[str, str], swap_enabled: bool, storage_class_name_set: bool
):
    if swap_enabled:
        assert selector["materialize.cloud/swap"] == "true"
        assert "materialize.cloud/scratch-fs" not in selector
    elif storage_class_name_set:
        assert selector["materialize.cloud/scratch-fs"] == "true"
        assert "materialize.cloud/swap" not in selector
    else:
        assert "materialize.cloud/swap" not in selector
        assert "materialize.cloud/scratch-fs" not in selector


EXPECTED_CLUSTERD_SYSCTLS = {
    "net.ipv4.tcp_keepalive_time": "300",
    "net.ipv4.tcp_keepalive_intvl": "30",
    "net.ipv4.tcp_keepalive_probes": "3",
}


def validate_clusterd_pod(pod: dict[str, Any], version: MzVersion) -> None:
    """Validate always-on properties of a clusterd pod."""
    if version >= MzVersion.parse_mz("v26.24.0-dev.0"):
        sysctls = {
            s["name"]: s["value"]
            for s in pod["spec"].get("securityContext", {}).get("sysctls", [])
        }
        for name, expected in EXPECTED_CLUSTERD_SYSCTLS.items():
            actual = sysctls.get(name)
            assert (
                actual == expected
            ), f"Expected sysctl {name}={expected}, but got {actual}"


def validate_container_resources(
    resources: dict[str, dict[str, str]],
    swap_enabled: bool,
):
    if swap_enabled:
        assert resources["requests"]["memory"] != resources["limits"]["memory"]
    else:
        assert resources["requests"]["memory"] == resources["limits"]["memory"]


class SwapEnabledGlobal(Modification):
    @classmethod
    def values(cls, version: MzVersion) -> list[Any]:
        return [True, False]

    @classmethod
    def default(cls) -> Any:
        return True

    def modify(self, definition: dict[str, Any]) -> None:
        definition["operator"]["operator"]["clusters"]["swap_enabled"] = self.value

    def validate(self, mods: dict[type[Modification], Any]) -> None:
        version = MzVersion.parse_mz(mods[EnvironmentdImageRef])
        if version < MzVersion.parse_mz("v0.158.0"):
            return

        orchestratord = get_orchestratord_data()
        args = orchestratord["items"][0]["spec"]["containers"][0]["args"]
        cluster_replica_sizes = json.loads(
            next(
                arg
                for arg in args
                if arg.startswith("--environmentd-cluster-replica-sizes=")
            ).split("=", 1)[1]
        )
        for size in cluster_replica_sizes.values():
            validate_cluster_replica_size(size, self.value, mods[StorageClass])

        labels = {
            "cluster.environmentd.materialize.cloud/type": "cluster",
            "cluster.environmentd.materialize.cloud/cluster-id": "s2",
            "cluster.environmentd.materialize.cloud/replica-id": "s1",
            "materialize.cloud/organization-name": "12345678-1234-1234-1234-123456789012",
        }

        def check_pods() -> None:
            clusterd = get_pod_data(labels)["items"][0]
            validate_clusterd_pod(clusterd, version)

            resources = clusterd["spec"]["containers"][0]["resources"]
            validate_container_resources(resources, self.value)

            node_selector = clusterd["spec"]["nodeSelector"]
            # pulling this one out, since it isn't in the cluster size's selector
            assert node_selector["workload"] == "materialize-instance"
            validate_node_selector(node_selector, self.value, mods[StorageClass])

        # Clusterd can take a while to start up
        retry(check_pods, 120)

        # TODO check that pods can actually use swap


class StorageClass(Modification):
    @classmethod
    def values(cls, version: MzVersion) -> list[Any]:
        return [True, False]

    @classmethod
    def default(cls) -> Any:
        return False

    def modify(self, definition: dict[str, Any]) -> None:
        if self.value:
            definition["operator"]["storage"]["storageClass"][
                "name"
            ] = "openebs-lvm-instance-store-ext4"

    def validate(self, mods: dict[type[Modification], Any]) -> None:
        version = MzVersion.parse_mz(mods[EnvironmentdImageRef])
        if version < MzVersion.parse_mz("v0.157.0"):
            return

        orchestratord = get_orchestratord_data()
        args = orchestratord["items"][0]["spec"]["containers"][0]["args"]
        cluster_replica_sizes = json.loads(
            next(
                arg
                for arg in args
                if arg.startswith("--environmentd-cluster-replica-sizes=")
            ).split("=", 1)[1]
        )
        for size in cluster_replica_sizes.values():
            validate_cluster_replica_size(size, mods[SwapEnabledGlobal], self.value)
        labels = {
            "cluster.environmentd.materialize.cloud/type": "cluster",
            "cluster.environmentd.materialize.cloud/cluster-id": "s2",
            "cluster.environmentd.materialize.cloud/replica-id": "s1",
            "materialize.cloud/organization-name": "12345678-1234-1234-1234-123456789012",
        }

        def check_pods() -> None:
            clusterd = get_pod_data(labels)["items"][0]
            validate_clusterd_pod(clusterd, version)

            resources = clusterd["spec"]["containers"][0]["resources"]
            validate_container_resources(resources, mods[SwapEnabledGlobal])

            node_selector = clusterd["spec"]["nodeSelector"]
            # checking this one separately, since it isn't in the cluster size's selector
            assert node_selector["workload"] == "materialize-instance"
            validate_node_selector(node_selector, mods[SwapEnabledGlobal], self.value)

        # Clusterd can take a while to start up
        retry(check_pods, 5)


class ClusterdCpu(Modification):
    # The default cluster (s2) uses the "25cc" size.
    DEFAULT_SIZE_NAME = "25cc"

    @classmethod
    def values(cls, version: MzVersion) -> list[Any]:
        return [
            None,
            # These values were chosen to be different from the defaults,
            # with the cpu_request lower than the limit,
            # and to be small enough to fit in developer laptops.
            {"cpu_limit": 1.5},
            {"cpu_request": 0.25},
            {"cpu_limit": 1.5, "cpu_request": 0.25},
        ]

    @classmethod
    def default(cls) -> Any:
        return None

    def modify(self, definition: dict[str, Any]) -> None:
        if self.value is None:
            return

        default_size = definition["operator"]["operator"]["clusters"]["sizes"][
            self.DEFAULT_SIZE_NAME
        ]

        default_size.pop("cpu_limit", None)
        default_size.pop("cpu_request", None)
        for key, val in self.value.items():
            default_size[key] = val

    def validate(self, mods: dict[type[Modification], Any]) -> None:
        version = MzVersion.parse_mz(mods[EnvironmentdImageRef])
        environmentd_supports_cpu_request = version >= MzVersion.parse_mz(
            "v26.15.0-dev.0"
        )

        environmentd = get_environmentd_data()
        args = environmentd["items"][0]["spec"]["containers"][0]["args"]
        cluster_replica_sizes_arg = next(
            (arg for arg in args if arg.startswith("--cluster-replica-sizes=")),
            None,
        )
        assert (
            cluster_replica_sizes_arg is not None
        ), f"Expected --cluster-replica-sizes in environmentd args, but only found: {args}"

        cluster_replica_sizes = json.loads(cluster_replica_sizes_arg.split("=", 1)[1])
        default_size = cluster_replica_sizes[self.DEFAULT_SIZE_NAME]

        # Verify the args passed to environmentd are what we expect.

        if self.value is None or "cpu_limit" not in self.value:
            # expect the value from the helm chart
            expected_arg_cpu_limit = "0.5"
        else:
            expected_arg_cpu_limit = self.value["cpu_limit"]

        if self.value is None or "cpu_request" not in self.value:
            # expect the value from the helm chart
            expected_arg_cpu_request = None
        else:
            expected_arg_cpu_request = self.value["cpu_request"]

        # compare as strings to avoid float issues
        assert str(default_size.get("cpu_request")) == str(expected_arg_cpu_request), (
            f"Expected arg cpu request {expected_arg_cpu_request}, "
            f"but got {default_size.get('cpu_request')}"
        )
        assert str(default_size.get("cpu_limit")) == str(expected_arg_cpu_limit), (
            f"Expected arg cpu limit {expected_arg_cpu_limit}, "
            f"but got {default_size.get('cpu_limit')}"
        )

        # Verify clusterd pods have correct CPU resources.

        def cpu_to_k8s_quantity(cpu: float | None) -> str | None:
            if cpu is None:
                return None
            return f"{int(cpu * 1000)}m"

        expected_pod_cpu_limit = cpu_to_k8s_quantity(default_size.get("cpu_limit"))
        if environmentd_supports_cpu_request:
            expected_pod_cpu_request = cpu_to_k8s_quantity(
                # We fall back to cpu_limit if we have no request,
                # so that the pod gets some guaranteed resources.
                default_size.get("cpu_request", default_size.get("cpu_limit")),
            )
        else:
            expected_pod_cpu_request = expected_pod_cpu_limit

        labels = {
            "cluster.environmentd.materialize.cloud/type": "cluster",
            "cluster.environmentd.materialize.cloud/cluster-id": "s2",
            "cluster.environmentd.materialize.cloud/replica-id": "s1",
            "materialize.cloud/organization-name": "12345678-1234-1234-1234-123456789012",
        }

        def check_pods() -> None:
            clusterd = get_pod_data(labels)["items"][0]
            validate_clusterd_pod(clusterd, version)
            resources = clusterd["spec"]["containers"][0]["resources"]
            actual_cpu_request = resources.get("requests", {}).get("cpu")
            actual_cpu_limit = resources.get("limits", {}).get("cpu")
            assert actual_cpu_request == expected_pod_cpu_request, (
                f"Expected pod cpu request {expected_pod_cpu_request}, "
                f"but got {actual_cpu_request}: {resources}"
            )
            assert actual_cpu_limit == expected_pod_cpu_limit, (
                f"Expected pod cpu limit {expected_pod_cpu_limit}, "
                f"but got {actual_cpu_limit}: {resources}"
            )

        retry(check_pods, 120)


class EnvironmentdResources(Modification):
    @classmethod
    def values(cls, version: MzVersion) -> list[Any]:
        return [
            None,
            {
                "limits": {
                    "cpu": "1",
                    "memory": "1Gi",
                },
                "requests": {
                    "cpu": "1",
                    "memory": "1Gi",
                },
            },
        ]

    @classmethod
    def default(cls) -> Any:
        return None

    def modify(self, definition: dict[str, Any]) -> None:
        definition["materialize"]["spec"][
            "environmentdResourceRequirements"
        ] = self.value

    def validate(self, mods: dict[type[Modification], Any]) -> None:
        expected = self.value
        if self.value is None:
            expected = {
                "limits": {
                    "memory": "4Gi",
                },
                "requests": {
                    "cpu": "1",
                    "memory": "4095Mi",
                },
            }

        def check_pods() -> None:
            environmentd = get_environmentd_data()["items"][0]

            resources = environmentd["spec"]["containers"][0]["resources"]
            assert (
                resources == expected
            ), f"Expected environmentd resources {expected}, but got {resources}"

        retry(check_pods, 120)


class BalancerdResources(Modification):
    @classmethod
    def values(cls, version: MzVersion) -> list[Any]:
        return [
            None,
            {
                "limits": {
                    "cpu": "1",
                    "memory": "512Mi",
                },
                "requests": {
                    "cpu": "1",
                    "memory": "512Mi",
                },
            },
        ]

    @classmethod
    def default(cls) -> Any:
        return None

    def modify(self, definition: dict[str, Any]) -> None:
        definition["materialize"]["spec"]["balancerdResourceRequirements"] = self.value

    def validate(self, mods: dict[type[Modification], Any]) -> None:
        version = MzVersion.parse_mz(mods[EnvironmentdImageRef])
        if version < MzVersion.parse_mz("v0.158.0"):
            return

        if mods[BalancerdEnabled] == False:
            return

        expected = self.value
        if self.value is None:
            expected = {
                "limits": {
                    "memory": "256Mi",
                },
                "requests": {
                    "cpu": "500m",
                    "memory": "256Mi",
                },
            }

        def check_pods() -> None:
            balancerd = get_balancerd_data()["items"][0]

            resources = balancerd["spec"]["containers"][0]["resources"]
            assert (
                resources == expected
            ), f"Expected balancerd resources {expected}, but got {resources}"

        retry(check_pods, 120)


class ConsoleResources(Modification):
    @classmethod
    def values(cls, version: MzVersion) -> list[Any]:
        return [
            None,
            {
                "limits": {
                    "cpu": "100m",
                    "memory": "128Mi",
                },
                "requests": {
                    "cpu": "100m",
                    "memory": "128Mi",
                },
            },
        ]

    @classmethod
    def default(cls) -> Any:
        return None

    def modify(self, definition: dict[str, Any]) -> None:
        definition["materialize"]["spec"]["consoleResourceRequirements"] = self.value

    def validate(self, mods: dict[type[Modification], Any]) -> None:
        if mods[ConsoleEnabled] == False:
            return
        expected = self.value
        if self.value is None:
            expected = {
                "limits": {
                    "memory": "256Mi",
                },
                "requests": {
                    "cpu": "500m",
                    "memory": "256Mi",
                },
            }

        def check_pods() -> None:
            console_data = get_console_data()
            assert console_data["items"], "No console pods found, expected at least one"
            console = console_data["items"][0]

            resources = console["spec"]["containers"][0]["resources"]
            assert (
                resources == expected
            ), f"Expected console resources {expected}, but got {resources}"

        retry(check_pods, 360)


class BalancerdExternalDnsNames(Modification):
    # The operator surfaces the balancerd external certificate's DNS names to
    # the console's `app-config.json` as `balancerd_dns_names`, so the console
    # knows which hostnames balancerd serves.
    DNS_NAMES = ["balancerd.example.com"]

    @classmethod
    def values(cls, version: MzVersion) -> list[Any]:
        return [None, cls.DNS_NAMES]

    @classmethod
    def default(cls) -> Any:
        return None

    def modify(self, definition: dict[str, Any]) -> None:
        if self.value is not None:
            definition["materialize"]["spec"]["balancerdExternalCertificateSpec"] = {
                "dnsNames": self.value,
            }

    def validate(self, mods: dict[type[Modification], Any]) -> None:
        # `balancerd_dns_names` was added to the console app config in v26.27;
        # older orchestratord builds don't surface the cert spec's DNS names.
        if MzVersion.parse_mz(mods[EnvironmentdImageRef]) < MzVersion.parse_mz(
            "v26.27.0-dev.0"
        ):
            return
        # Without a console there's no app config to inspect.
        if not mods[ConsoleEnabled]:
            return

        def check() -> None:
            app_config = get_console_app_config()
            actual = app_config.get("balancerd_dns_names")
            assert (
                actual == self.value
            ), f"Expected balancerd_dns_names {self.value}, but got {actual}: {app_config}"

        # The console is reconciled last and the configmap update is async.
        retry(check, 360)


class RecommendedK8sLabels(Modification):
    @classmethod
    def values(cls, version: MzVersion) -> list[Any]:
        return [None]

    @classmethod
    def default(cls) -> Any:
        return None

    def modify(self, definition: dict[str, Any]) -> None:
        pass

    def validate(self, mods: dict[type[Modification], Any]) -> None:
        if MzVersion.parse_mz(mods[EnvironmentdImageRef]) < MzVersion.parse_mz(
            "v26.24.0"
        ):
            return

        def get(kind: str, name: str) -> dict[str, Any]:
            return json.loads(
                spawn.capture(
                    [
                        "kubectl",
                        "get",
                        kind,
                        name,
                        "-n",
                        "materialize-environment",
                        "-o",
                        "json",
                    ]
                )
            )

        def check() -> None:
            pod = get_environmentd_data()["items"][0]
            statefulset = get(
                "statefulset", pod["metadata"]["labels"]["materialize.cloud/name"]
            )
            service = get("service", statefulset["spec"]["serviceName"])
            for kind, obj in (("statefulset", statefulset), ("service", service)):
                actual = obj["metadata"].get("labels", {}).get("app.kubernetes.io/name")
                assert (
                    actual == "environmentd"
                ), f"Expected app.kubernetes.io/name=environmentd on {kind}/{obj['metadata']['name']}, got {actual!r}"

        retry(check, 120)


class AuthenticatorKind(Modification):
    @classmethod
    def values(cls, version: MzVersion) -> list[Any]:
        # Test None, Password (v0.147.7+), Sasl, and Oidc
        result = ["None"]
        if version >= MzVersion.parse_mz("v0.147.7"):
            result.append("Password")
        if version >= MzVersion.parse_mz("v26.0.0"):
            result.append("Sasl")
        if version >= MzVersion.parse_mz("v26.16.0-dev.0"):
            result.append("Oidc")
        return result

    @classmethod
    def default(cls) -> Any:
        return "None"

    def modify(self, definition: dict[str, Any]) -> None:
        definition["materialize"]["spec"]["authenticatorKind"] = self.value
        if self.value == "Password" or self.value == "Sasl" or self.value == "Oidc":
            definition["secret"]["stringData"][
                "external_login_password_mz_system"
            ] = "superpassword"
        elif "external_login_password_mz_system" in definition["secret"]["stringData"]:
            del definition["secret"]["stringData"]["external_login_password_mz_system"]

    def validate(self, mods: dict[type[Modification], Any]) -> None:
        environmentd = get_environmentd_data()
        name = environmentd["items"][0]["metadata"]["name"]
        process = None
        version = MzVersion.parse_mz(mods[EnvironmentdImageRef])

        if self.value == "Password" and version <= MzVersion.parse_mz("v0.147.6"):
            return

        if self.value == "Sasl" and version < MzVersion.parse_mz("v26.0.0"):
            return

        if self.value == "Oidc" and version < MzVersion.parse_mz("v26.16.0-dev.0"):
            return

        port = (
            6875
            if (version >= MzVersion.parse_mz("v0.147.0") and self.value == "Password")
            or (version >= MzVersion.parse_mz("v26.0.0") and self.value == "Sasl")
            or (
                version >= MzVersion.parse_mz("v26.16.0-dev.0") and self.value == "Oidc"
            )
            else 6877
        )
        for i in range(120):
            process = subprocess.Popen(
                [
                    "kubectl",
                    "port-forward",
                    f"pod/{name}",
                    "-n",
                    "materialize-environment",
                    f"{port}:{port}",
                ],
                preexec_fn=os.setpgrp,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
            )
            time.sleep(1)
            process.poll()
            assert process.stdout
            line = process.stdout.readline()
            if "Forwarding from" in line:
                break
            else:
                print("Port-forward failed, retrying")
        else:
            spawn.capture(
                [
                    "kubectl",
                    "describe",
                    "pod",
                    "-l",
                    "app=environmentd",
                    "-n",
                    "materialize-environment",
                ]
            )
            raise ValueError(
                "Port-forwarding never worked, environmentd status:\n{status}"
            )

        time.sleep(1)
        try:
            # Verify listener configuration
            listeners_cm = spawn.capture(
                [
                    "kubectl",
                    "get",
                    "cm",
                    "-n",
                    "materialize-environment",
                    "-o",
                    "name",
                ],
                stderr=subprocess.STDOUT,
            ).splitlines()
            listeners_cm = [cm for cm in listeners_cm if "listeners" in cm]
            if listeners_cm:
                listeners_json = spawn.capture(
                    [
                        "kubectl",
                        "get",
                        listeners_cm[0],
                        "-n",
                        "materialize-environment",
                        "-o",
                        "jsonpath={.data.listeners\\.json}",
                    ],
                    stderr=subprocess.STDOUT,
                )
                import json

                listeners = json.loads(listeners_json)

                # Verify SQL listener authenticator
                sql_auth = (
                    listeners.get("sql", {})
                    .get("external", {})
                    .get("authenticator_kind")
                )
                http_auth = (
                    listeners.get("http", {})
                    .get("external", {})
                    .get("authenticator_kind")
                )

                if self.value == "Sasl":
                    assert (
                        sql_auth == "Sasl"
                    ), f"Expected SQL listener to use Sasl, got {sql_auth}"
                    assert (
                        http_auth == "Password"
                    ), f"Expected HTTP listener to use Password with Sasl mode, got {http_auth}"
                    print(f"SASL mode verified: SQL={sql_auth}, HTTP={http_auth}")
                elif self.value == "Password":
                    assert (
                        sql_auth == "Password"
                    ), f"Expected SQL listener to use Password, got {sql_auth}"
                    assert (
                        http_auth == "Password"
                    ), f"Expected HTTP listener to use Password, got {http_auth}"
                    print(f"Password mode verified: SQL={sql_auth}, HTTP={http_auth}")
                elif self.value == "Oidc":
                    assert (
                        sql_auth == "Oidc"
                    ), f"Expected SQL listener to use Oidc, got {sql_auth}"
                    assert (
                        http_auth == "Oidc"
                    ), f"Expected HTTP listener to use Oidc, got {http_auth}"
                    print(f"Oidc mode verified: SQL={sql_auth}, HTTP={http_auth}")
                elif self.value == "None":
                    assert (
                        sql_auth == "None"
                    ), f"Expected SQL listener to use None, got {sql_auth}"
                    assert (
                        http_auth == "None"
                    ), f"Expected HTTP listener to use None, got {http_auth}"
                    print(f"None mode verified: SQL={sql_auth}, HTTP={http_auth}")

            # TODO: Figure out why this is not working in CI, but works locally
            pass
            # psycopg.connect(
            #     host="127.0.0.1",
            #     user="mz_system",
            #     password="superpassword" if (self.value == "Password" or self.value == "Sasl") else None,
            #     dbname="materialize",
            #     port=port,
            # )
        finally:
            os.killpg(os.getpgid(process.pid), signal.SIGTERM)


class RolloutStrategy(Modification):
    @classmethod
    def values(cls, version: MzVersion) -> list[Any]:
        return [
            "WaitUntilReady",
            "ManuallyPromote",
            "ImmediatelyPromoteCausingDowntime",
        ]

    @classmethod
    def default(cls) -> Any:
        return "WaitUntilReady"

    def modify(self, definition: dict[str, Any]) -> None:
        definition["materialize"]["spec"]["rolloutStrategy"] = self.value

    def validate(self, mods: dict[type[Modification], Any]) -> None:
        # This is validated in post_run_check
        return


class MaterializeCRDVersion(Modification):
    @classmethod
    def values(cls, version: MzVersion) -> list[Any]:
        return [
            "materialize.cloud/v1alpha1",
            "materialize.cloud/v1",
        ]

    @classmethod
    def default(cls) -> Any:
        return "materialize.cloud/v1"

    def modify(self, definition: dict[str, Any]) -> None:
        if self.value == "materialize.cloud/v1" and operator_supports_v1(definition):
            # The operator only installs and serves the v1 CRD version when
            # explicitly asked to.
            enable_v1_crd(definition)
            definition["materialize"]["apiVersion"] = self.value
        else:
            # Older versions do not support v1, and without installV1CRD the
            # operator does not serve it.
            definition["materialize"]["apiVersion"] = "materialize.cloud/v1alpha1"

    def validate(self, mods: dict[type[Modification], Any]) -> None:
        # This should be OK without additional validation, as we check we
        # deployed in post_run_check and check the installed CRD versions in
        # check_crd_versions.
        return


class CertificateSource(Modification):
    SECRET_NAME = "orchestratord-custom-cert"

    @classmethod
    def values(cls, version: MzVersion) -> list[Any]:
        return ["cert-manager", "secret"]

    @classmethod
    def default(cls) -> Any:
        return "cert-manager"

    def modify(self, definition: dict[str, Any]) -> None:
        # The certificate is only used to serve the conversion webhook, which
        # is only installed together with the v1 CRD.
        if operator_supports_v1(definition):
            enable_v1_crd(definition)
        definition["operator"]["operator"]["certificate"]["source"] = self.value
        if self.value == "secret":
            definition["operator"]["operator"]["certificate"][
                "secretName"
            ] = self.SECRET_NAME
            self._create_cert_secret()

    @classmethod
    def _create_cert_secret(cls) -> None:
        dns_name = "operator-materialize-operator.materialize.svc"

        with tempfile.TemporaryDirectory() as tmpdir:
            ca_key = os.path.join(tmpdir, "ca.key")
            ca_crt = os.path.join(tmpdir, "ca.crt")
            tls_key = os.path.join(tmpdir, "tls.key")
            tls_crt = os.path.join(tmpdir, "tls.crt")
            csr_path = os.path.join(tmpdir, "server.csr")
            ext_path = os.path.join(tmpdir, "ext.cnf")

            # Generate CA key and self-signed cert
            spawn.runv(
                [
                    "openssl",
                    "req",
                    "-x509",
                    "-newkey",
                    "rsa:2048",
                    "-keyout",
                    ca_key,
                    "-out",
                    ca_crt,
                    "-days",
                    "1",
                    "-nodes",
                    "-subj",
                    "/CN=Test CA",
                ]
            )

            # Generate server key
            spawn.runv(
                [
                    "openssl",
                    "genpkey",
                    "-algorithm",
                    "rsa",
                    "-pkeyopt",
                    "rsa_keygen_bits:2048",
                    "-out",
                    tls_key,
                ]
            )

            # Generate CSR
            spawn.runv(
                [
                    "openssl",
                    "req",
                    "-new",
                    "-key",
                    tls_key,
                    "-out",
                    csr_path,
                    "-subj",
                    f"/CN={dns_name}",
                ]
            )

            # Write extension file for SAN
            with open(ext_path, "w") as f:
                f.write(f"subjectAltName=DNS:{dns_name}\n")

            # Sign CSR with CA
            spawn.runv(
                [
                    "openssl",
                    "x509",
                    "-req",
                    "-in",
                    csr_path,
                    "-CA",
                    ca_crt,
                    "-CAkey",
                    ca_key,
                    "-CAcreateserial",
                    "-out",
                    tls_crt,
                    "-days",
                    "1",
                    "-extfile",
                    ext_path,
                ]
            )

            # Delete existing secret if present
            try:
                spawn.capture(
                    [
                        "kubectl",
                        "delete",
                        "secret",
                        cls.SECRET_NAME,
                        "-n",
                        "materialize",
                    ],
                    stderr=subprocess.DEVNULL,
                )
            except subprocess.CalledProcessError:
                pass

            # Create the secret with ca.crt, tls.crt, and tls.key
            spawn.runv(
                [
                    "kubectl",
                    "create",
                    "secret",
                    "generic",
                    cls.SECRET_NAME,
                    f"--from-file=ca.crt={ca_crt}",
                    f"--from-file=tls.crt={tls_crt}",
                    f"--from-file=tls.key={tls_key}",
                    "-n",
                    "materialize",
                ]
            )

    def validate(self, mods: dict[type[Modification], Any]) -> None:
        def check() -> None:
            orchestratord = get_orchestratord_data()
            container = orchestratord["items"][0]["spec"]["containers"][0]
            if "--install-v1-crd" not in container["args"]:
                # The operator does not serve the conversion webhook (e.g. it
                # is too old to know the flag), so no certificate is mounted.
                return
            volumes = orchestratord["items"][0]["spec"].get("volumes") or []
            cert_volume = next(
                (v for v in volumes if v.get("name") == "certificate"),
                None,
            )
            assert cert_volume is not None, f"Expected certificate volume in {volumes}"

            secret_name = cert_volume["secret"]["secretName"]
            if self.value == "cert-manager":
                expected = "operator-materialize-operator-cert"
            else:
                expected = self.SECRET_NAME
            assert (
                secret_name == expected
            ), f"Expected certificate secret name '{expected}', got '{secret_name}'"

        retry(check, 120)


def operator_supports_v1(definition: dict[str, Any]):
    operator_version = MzVersion.parse(
        definition["operator"]["operator"]["image"]["tag"]
    )
    # v1 first ships in v26.30; no released self-managed operator serves
    # the v1 CRD, so anything older must use v1alpha1. Keep this in sync
    # with the release that actually introduces the v1 CRD.
    return operator_version >= MzVersion.parse("v26.30.0-dev.0")


def operator_serves_v1(definition: dict[str, Any]) -> bool:
    """Whether the operator in this definition installs and serves the v1 CRD.

    Even operators that support v1 only install it when the installV1CRD helm
    value is set."""
    return operator_supports_v1(definition) and bool(
        definition["operator"]["operator"]["args"].get("installV1CRD")
    )


def enable_v1_crd(definition: dict[str, Any]) -> None:
    """Make the operator install the v1 CRD and its conversion webhook."""
    assert operator_supports_v1(
        definition
    ), "the operator version is too old to install the v1 CRD"
    definition["operator"]["operator"]["args"]["installV1CRD"] = True


class Properties(Enum):
    Defaults = "defaults"
    Individual = "individual"
    Combine = "combine"


class Action(Enum):
    Noop = "noop"
    Upgrade = "upgrade"
    UpgradeChain = "upgrade-chain"


def workflow_documentation_defaults(
    c: Composition, parser: WorkflowArgumentParser
) -> None:
    parser.add_argument(
        "--tag",
        type=str,
        help="Custom version tag to use",
    )
    args = parser.parse_args()

    c.up(Service("mz-debug", idle=True))
    c.invoke("cp", "mz-debug:/usr/local/bin/mz-debug", ".")

    current_version = get_version(args.tag)

    # Following https://materialize.com/docs/installation/install-on-local-kind/
    # orchestratord test can't run against future versions, so ignore those
    # Pre-v26 versions don't have balancerd/console images on Docker Hub
    versions = buildkite.shard_list(
        list(
            reversed(
                [
                    version
                    for version in get_self_managed_versions()
                    if version < current_version
                    and version >= MzVersion.parse_mz("v26.0.0")
                ]
                + [current_version]
            )
        ),
        lambda version: str(version),
    )
    for version in versions:
        print(f"--- Running with defaults against {version}")
        dir = "my-local-mz"
        if os.path.exists(dir):
            shutil.rmtree(dir)
        os.mkdir(dir)
        recreate_kind_cluster()

        helm_install_cert_manager()

        shutil.copyfile(
            "misc/helm-charts/operator/values.yaml",
            os.path.join(dir, "sample-values.yaml"),
        )
        files = {
            "sample-postgres.yaml": "misc/helm-charts/testing/postgres.yaml",
            "sample-minio.yaml": "misc/helm-charts/testing/minio.yaml",
            "sample-materialize.yaml": "misc/helm-charts/testing/materialize.yaml",
        }

        for file, path in files.items():
            if version == current_version:
                shutil.copyfile(path, os.path.join(dir, file))
            else:
                content = download_repo_file_at_tag(path, str(version))
                with open(os.path.join(dir, file), "wb") as f:
                    f.write(content)

        with open(os.path.join(dir, "sample-values.yaml")) as f:
            sample_values = yaml.load(f, Loader=yaml.Loader)
        sample_values["operator"]["image"]["tag"] = str(current_version)
        helm_install_operator(sample_values, upgrade=False)

        spawn.runv(
            ["kubectl", "apply", "-f", os.path.join(dir, "sample-postgres.yaml")]
        )
        spawn.runv(["kubectl", "apply", "-f", os.path.join(dir, "sample-minio.yaml")])
        spawn.runv(["kubectl", "get", "all", "-n", "materialize"])

        wait_for_crd_established()
        spawn.runv(
            [
                "kubectl",
                "wait",
                "-n",
                "materialize",
                "--for=condition=Available",
                "--timeout=300s",
                "deployment/minio",
            ]
        )
        spawn.runv(
            [
                "kubectl",
                "wait",
                "-n",
                "materialize",
                "--for=condition=Available",
                "--timeout=300s",
                "deployment/postgres",
            ]
        )

        with open(os.path.join(dir, "sample-materialize.yaml")) as f:
            materialize_setup = list(yaml.load_all(f, Loader=yaml.Loader))
        assert len(materialize_setup) == 3

        if version == current_version:
            materialize_setup[2]["spec"][
                "environmentdImageRef"
            ] = f"materialize/environmentd:{version}"
        if version >= MzVersion.parse_mz("v26.0.0"):
            # Self-managed v25.1/2 don't require a license key yet
            materialize_setup[1]["stringData"]["license_key"] = os.environ[
                "MZ_CI_LICENSE_KEY"
            ]
        else:
            # TODO: Remove this part once environmentId is set in older versions
            materialize_setup[2]["spec"][
                "environmentId"
            ] = "12345678-1234-1234-1234-123456789013"

        with open(os.path.join(dir, "sample-materialize.yaml"), "w") as f:
            yaml.dump_all(materialize_setup, f, default_flow_style=False)

        spawn.runv(
            ["kubectl", "apply", "-f", os.path.join(dir, "sample-materialize.yaml")]
        )

        # This should finish quickly, see https://github.com/MaterializeInc/database-issues/issues/10099
        # The timeout is generous because the oldest supported environmentd
        # versions boot slowly and the operator can spend ~30s in a single
        # reconcile before it creates the console.
        for i in range(240):
            try:
                data = json.loads(
                    spawn.capture(
                        [
                            "kubectl",
                            "get",
                            "pod",
                            "-n",
                            "materialize-environment",
                            "-o",
                            "json",
                        ]
                    )
                )
                expected_pods = {
                    "environmentd": 1,
                    "clusterd": 2,
                    "balancerd": 2,
                    "console": 2,
                }
                actual_pods = {
                    "environmentd": 0,
                    "clusterd": 0,
                    "balancerd": 0,
                    "console": 0,
                }
                for item in data.get("items", []):
                    for container in item.get("status", {}).get(
                        "containerStatuses", []
                    ):
                        name = container.get("name")
                        assert name in expected_pods, f"Unexpected pod {name}"
                        state = container.get("state", {})
                        if "running" in state:
                            actual_pods[name] += 1
                if expected_pods == actual_pods:
                    spawn.runv(
                        [
                            "kubectl",
                            "get",
                            "pod",
                            "-n",
                            "materialize-environment",
                        ]
                    )
                    break
                else:
                    print(f"Current pods: {actual_pods}")

            except subprocess.CalledProcessError:
                pass
            time.sleep(1)
        else:
            spawn.runv(
                [
                    "kubectl",
                    "get",
                    "pod",
                    "-n",
                    "materialize-environment",
                ]
            )
            # Helps to debug
            spawn.runv(
                [
                    "kubectl",
                    "describe",
                    "pod",
                    "-l",
                    "app=environmentd",
                    "-n",
                    "materialize-environment",
                ]
            )
            raise ValueError("Never completed")
        run_mz_debug()


class ModSource:
    def __init__(self, mod_classes: list[type[Modification]]):
        self.mod_classes = mod_classes

    def next_mods(self, version: MzVersion) -> list[Modification]:
        raise NotImplementedError


class DefaultModSource(ModSource):
    def __init__(self, mod_classes: list[type[Modification]]):
        super().__init__(mod_classes)
        self.state = 0

    def next_mods(self, version: MzVersion) -> list[Modification]:
        if self.state == 0:
            self.state += 1
            return [cls(cls.default()) for cls in self.mod_classes]
        elif self.state == 1:
            self.state += 1
            return [NumMaterializeEnvironments(2)]
        else:
            raise StopIteration


class IndividualModSource(ModSource):
    def __init__(self, mod_classes: list[type[Modification]]):
        super().__init__(mod_classes)
        self._iters_by_version: dict[object, Iterator[list[Modification]]] = {}

    def _iter_values_for_version(
        self, version: MzVersion
    ) -> Iterator[list[Modification]]:
        for cls in self.mod_classes:
            for value in cls.values(version):
                yield [cls(value)]

    def next_mods(self, version: MzVersion) -> list[Modification]:
        it = self._iters_by_version.setdefault(
            version, self._iter_values_for_version(version)
        )
        try:
            return next(it)
        except StopIteration:
            del self._iters_by_version[version]
            raise


class CombineModSource(ModSource):
    def __init__(self, mod_classes: list[type[Modification]], rng: random.Random):
        super().__init__(mod_classes)
        self.rng = rng

    def next_mods(self, version: MzVersion) -> list[Modification]:
        return [
            cls(self.rng.choice(cls.good_values(version))) for cls in self.mod_classes
        ]


def make_mod_source(
    properties: Properties, mod_classes: list[type[Modification]], rng: random.Random
):
    if properties == Properties.Defaults:
        return DefaultModSource(mod_classes)
    elif properties == Properties.Individual:
        return IndividualModSource(mod_classes)
    elif properties == Properties.Combine:
        return CombineModSource(mod_classes, rng)
    else:
        raise ValueError(f"Unhandled properties: {properties}")


# Bump this version if the upgrade-downtime workflow is changed in a way that changes the results uploaded to test analytics
UPGRADE_DOWNTIME_SCENARIO_VERSION = "1.0.0"
# Used for uploading test analytics results
ORCHESTRATORD_TEST_VERSION = "1.0.0"


def upload_upgrade_downtime_to_test_analytics(
    composition: Composition,
    downtime_initial: float,
    downtime_upgrade: float,
    was_successful: bool,
) -> None:
    if not buildkite.is_in_buildkite():
        return

    test_analytics = TestAnalyticsDb(create_test_analytics_config(composition))
    test_analytics.builds.add_build_job(was_successful=was_successful)

    result_entries = [
        upgrade_downtime_result_storage.UpgradeDowntimeResultEntry(
            scenario="upgrade-downtime",
            scenario_version=UPGRADE_DOWNTIME_SCENARIO_VERSION,
            downtime_initial=downtime_initial,
            downtime_upgrade=downtime_upgrade,
        )
    ]

    test_analytics.upgrade_downtime_results.add_result(
        framework_version=ORCHESTRATORD_TEST_VERSION,
        results=result_entries,
    )

    try:
        test_analytics.submit_updates()
        print("Uploaded results.")
    except Exception as e:
        # An error during an upload must never cause the build to fail
        test_analytics.on_upload_failed(e)


def workflow_upgrade_downtime(c: Composition, parser: WorkflowArgumentParser) -> None:
    parser.add_argument(
        "--recreate-cluster",
        action=argparse.BooleanOptionalAction,
        help="Recreate cluster if it exists already",
    )
    parser.add_argument(
        "--tag",
        type=str,
        help="Custom version tag to use",
    )
    parser.add_argument(
        "--orchestratord-override",
        default=True,
        action=argparse.BooleanOptionalAction,
        help="Override orchestratord tag",
    )
    args = parser.parse_args()

    running = True
    downtimes: list[float] = []

    def measure_downtime() -> None:
        port_forward_process = None
        connect_port_forward = True
        try:
            start_time = time.time()
            while running:
                if connect_port_forward:
                    if port_forward_process:
                        os.killpg(os.getpgid(port_forward_process.pid), signal.SIGTERM)
                        port_forward_process = None
                    try:
                        balancerd_name = spawn.capture(
                            [
                                "kubectl",
                                "get",
                                "pods",
                                "-l",
                                "app=balancerd",
                                "-n",
                                "materialize-environment",
                                "-o",
                                "jsonpath={.items[0].metadata.name}",
                            ]
                        ).strip()
                    except subprocess.CalledProcessError:
                        # balancerd can take a bit to start up
                        time.sleep(1)
                        continue
                    port_forward_process = subprocess.Popen(
                        [
                            "kubectl",
                            "port-forward",
                            f"pod/{balancerd_name}",
                            "-n",
                            "materialize-environment",
                            "6875:6875",
                        ],
                        preexec_fn=os.setpgrp,
                    )
                    connect_port_forward = False
                time.sleep(1)
                try:
                    with psycopg.connect(
                        "postgres://materialize@127.0.0.1:6875/materialize",
                        autocommit=True,
                    ) as conn:
                        with conn.cursor() as cur:
                            cur.execute("SELECT 1")
                except psycopg.OperationalError:
                    connect_port_forward = True
                    continue
                runtime = time.time() - start_time - 1
                print(f"Time: {runtime}s")
                if runtime > 1:
                    downtimes.append(runtime)
                start_time = time.time()
        finally:
            if port_forward_process:
                os.killpg(os.getpgid(port_forward_process.pid), signal.SIGTERM)
        if len(downtimes) == 1:
            downtimes.insert(0, 0)

    definition = setup(c, args)
    init(definition)
    run(definition, False)
    thread = PropagatingThread(target=measure_downtime)
    thread.start()
    time.sleep(10)  # some time to make sure the thread runs fine
    request = str(uuid.uuid4())
    if definition["materialize"]["apiVersion"] == "materialize.cloud/v1alpha1":
        definition["materialize"]["spec"]["requestRollout"] = request
    definition["materialize"]["spec"]["forceRollout"] = request
    run(definition, False)
    time.sleep(120)  # some time to make sure there is no downtime later
    running = False
    thread.join()

    assert len(downtimes) == 2, f"Wrong number of downtimes: {downtimes}"

    test_failed = False
    # TODO: Reduce to 15 s when https://linear.app/materializeinc/issue/DB-106 is fixed
    max_downtime = 120
    upgrade_downtime = downtimes[-1]
    if upgrade_downtime > max_downtime:
        print(
            f"SELECT 1 after upgrade took more than {max_downtime}s: {upgrade_downtime}s"
        )
        test_failed = True

    upload_upgrade_downtime_to_test_analytics(
        c, downtimes[0], downtimes[1], not test_failed
    )
    assert not test_failed


def workflow_balancer(c: Composition, parser: WorkflowArgumentParser) -> None:
    parser.add_argument(
        "--recreate-cluster",
        action=argparse.BooleanOptionalAction,
        help="Recreate cluster if it exists already",
    )
    parser.add_argument(
        "--tag",
        type=str,
        help="Custom version tag to use",
    )
    parser.add_argument(
        "--orchestratord-override",
        default=True,
        action=argparse.BooleanOptionalAction,
        help="Override orchestratord tag",
    )
    args = parser.parse_args()
    definition = setup(c, args)
    init(definition)
    run_balancer(definition, False)


def get_materialize_v1alpha1() -> dict[str, Any]:
    """Get the first Materialize resource at v1alpha1."""
    data = json.loads(
        spawn.capture(
            [
                "kubectl",
                "get",
                "materializes.v1alpha1.materialize.cloud",
                "-n",
                "materialize-environment",
                "-o",
                "json",
            ],
            stderr=subprocess.DEVNULL,
        )
    )
    return data["items"][0]


def get_materialize_at_stored_version() -> dict[str, Any]:
    """Get the first Materialize resource at the stored version (currently v1alpha1)."""
    return get_materialize_v1alpha1()


def get_materialize_status_at_stored_version() -> dict[str, Any] | None:
    """Get the status of the first Materialize resource at the stored version."""
    return get_materialize_at_stored_version().get("status")


def get_materialize_v1() -> dict[str, Any]:
    """Get the first Materialize resource at v1."""
    data = json.loads(
        spawn.capture(
            [
                "kubectl",
                "get",
                "materializes.v1.materialize.cloud",
                "-n",
                "materialize-environment",
                "-o",
                "json",
            ],
            stderr=subprocess.DEVNULL,
        )
    )
    return data["items"][0]


def workflow_v1_opt_in(
    c: Composition,
    parser: WorkflowArgumentParser,
) -> None:
    """Test that applying a v1 resource triggers reconciliation only
    when the spec has changed, and not when it is unchanged.

    The conversion webhook computes a rollout hash from the v1 spec.
    When converting to v1alpha1 for storage, it derives a deterministic
    requestRollout UUID from the hash. When the spec is unchanged, the
    derived UUID matches lastCompletedRolloutRequest, so no rollout occurs.
    When the spec changes, the derived UUID differs, triggering a rollout.
    """
    parser.add_argument(
        "--recreate-cluster",
        action=argparse.BooleanOptionalAction,
        help="Recreate cluster if it exists already",
    )
    parser.add_argument(
        "--tag",
        type=str,
        help="Custom version tag to use",
    )
    parser.add_argument(
        "--orchestratord-override",
        default=True,
        action=argparse.BooleanOptionalAction,
        help="Override orchestratord tag",
    )
    args = parser.parse_args()

    definition = setup(c, args)
    enable_v1_crd(definition)

    # Step 1: Deploy with v1, complete initial rollout.
    definition["materialize"]["apiVersion"] = "materialize.cloud/v1"
    init(definition)
    run(definition, False)
    print("Initial v1 deployment completed")

    # Record the initial v1alpha1 status.
    mz_v1 = get_materialize_v1alpha1()
    initial_request_rollout = mz_v1["spec"]["requestRollout"]
    initial_last_completed_rollout_request = mz_v1["status"][
        "lastCompletedRolloutRequest"
    ]
    assert (
        initial_request_rollout == initial_last_completed_rollout_request
    ), f"Expected completed rollout: requestRollout={initial_request_rollout} != lastCompletedRolloutRequest={initial_last_completed_rollout_request}"
    print(f"Initial requestRollout: {initial_request_rollout}")

    # Step 2: Re-apply the same spec at v1 (no changes).
    # The conversion webhook should compute the same hash, deriving the
    # same requestRollout UUID, so no new rollout should occur.
    print("Re-applying same spec at v1 (expecting no rollout)...")
    apply_materialize(definition)

    # Wait a bit and verify that no new rollout was triggered.
    time.sleep(30)
    mz_v1 = get_materialize_v1alpha1()
    noop_request_rollout = mz_v1["spec"]["requestRollout"]
    assert (
        noop_request_rollout == initial_request_rollout
    ), f"Expected requestRollout unchanged, but changed from {initial_request_rollout} to {noop_request_rollout}"
    noop_last_completed_rollout_request = mz_v1["status"]["lastCompletedRolloutRequest"]
    assert (
        noop_last_completed_rollout_request == initial_last_completed_rollout_request
    ), f"Expected lastCompletedRolloutRequest unchanged, but changed from {initial_last_completed_rollout_request} to {noop_last_completed_rollout_request}"
    print("Confirmed: no rollout triggered by v1 apply with no changes")

    # Step 3: Apply at v1 with a spec change (extra env var).
    # The conversion webhook should compute a different hash, deriving a
    # different requestRollout UUID, triggering a rollout.
    print("Applying v1 with changed environmentdExtraEnv (expecting rollout)...")
    definition["materialize"]["spec"]["environmentdExtraEnv"] = [
        {"name": "V1_OPT_IN_TEST", "value": "true"},
    ]
    run(definition, False)

    mz_v1 = get_materialize_v1alpha1()
    changed_request_rollout = mz_v1["spec"]["requestRollout"]
    assert (
        changed_request_rollout != initial_request_rollout
    ), f"Expected requestRollout to change after spec change, but still {changed_request_rollout}"

    def check_rollout_complete():
        mz_v1 = get_materialize_v1alpha1()
        changed_last_completed_rollout_request = mz_v1["status"][
            "lastCompletedRolloutRequest"
        ]
        assert (
            changed_last_completed_rollout_request == changed_request_rollout
        ), f"Expected rollout to complete: requestRollout={changed_request_rollout} != lastCompletedRolloutRequest={changed_last_completed_rollout_request}"

    retry(check_rollout_complete, 120)
    print(
        f"Confirmed: rollout triggered by v1 spec change. "
        f"requestRollout changed from {initial_request_rollout} to {changed_request_rollout}"
    )
    print("v1 opt-in test PASSED")


def apply_server_side(
    obj: dict[str, Any],
    field_manager: str | None = None,
    force_conflicts: bool = True,
) -> None:
    """Server-side apply a single object via kubectl, retrying while the
    conversion webhook is still coming up."""
    cmd = ["kubectl", "apply", "--server-side", "-f", "-"]
    if force_conflicts:
        cmd.append("--force-conflicts")
    if field_manager:
        cmd.append(f"--field-manager={field_manager}")
    yaml_str = yaml.dump(obj)
    print(f"Attempting to apply server-side:\n{yaml_str}")
    transient_webhook_errors = (
        "connection refused",
        "deadline exceeded",
        "i/o timeout",
    )
    max_attempts = 120
    for attempt in range(max_attempts):
        result = subprocess.run(cmd, input=yaml_str.encode(), capture_output=True)
        if result.returncode == 0:
            return
        stderr_str = result.stderr.decode(errors="replace")
        if attempt < max_attempts - 1 and any(
            err in stderr_str for err in transient_webhook_errors
        ):
            print(f"Webhook not yet ready (attempt {attempt + 1}), retrying...")
            time.sleep(2)
            continue
        print(f"Failed to apply: {result.stdout}\nSTDERR:{result.stderr}")
        raise subprocess.CalledProcessError(
            result.returncode,
            result.args,
            output=result.stdout,
            stderr=result.stderr,
        )


def workflow_server_side_apply(
    c: Composition,
    parser: WorkflowArgumentParser,
) -> None:
    """Test `kubectl apply --server-side` of Materialize resources across CRD
    versions.

    When an object has managed fields recorded at another CRD version,
    server-side apply round-trips each field manager's owned subset of the
    object through the conversion webhook while reconciling field ownership.
    Those subsets are partial objects that lack required fields, so the
    conversion webhook must convert them field-wise instead of rejecting
    them.

    Regression test for the conversion webhook rejecting such partial objects
    with "missing field `environmentdImageRef`", which broke every
    server-side apply of a v1 resource whose managed fields were recorded at
    v1alpha1 (the situation after enabling the v1 CRD on an existing
    v1alpha1-managed instance).
    """
    parser.add_argument(
        "--recreate-cluster",
        action=argparse.BooleanOptionalAction,
        help="Recreate cluster if it exists already",
    )
    parser.add_argument(
        "--tag",
        type=str,
        help="Custom version tag to use",
    )
    parser.add_argument(
        "--orchestratord-override",
        default=True,
        action=argparse.BooleanOptionalAction,
        help="Override orchestratord tag",
    )
    args = parser.parse_args()

    definition = setup(c, args)
    enable_v1_crd(definition)

    # Step 1: Deploy at v1alpha1 with client-side apply and complete the
    # initial rollout. This records the applier's and orchestratord's managed
    # fields at v1alpha1.
    definition["materialize"]["apiVersion"] = "materialize.cloud/v1alpha1"
    init(definition)
    run(definition, False)
    print("Initial v1alpha1 deployment completed")

    mz = get_materialize_v1alpha1()
    initial_request_rollout = mz["spec"]["requestRollout"]

    # Guard that the test exercises what it claims to: there must be managed
    # fields recorded at v1alpha1 for the server-side apply below to prune.
    managed_fields = json.loads(
        spawn.capture(
            [
                "kubectl",
                "get",
                "materializes.v1alpha1.materialize.cloud",
                mz["metadata"]["name"],
                "-n",
                "materialize-environment",
                "--show-managed-fields",
                "-o",
                "jsonpath={.metadata.managedFields}",
            ]
        )
    )
    assert any(
        entry["apiVersion"] == "materialize.cloud/v1alpha1" for entry in managed_fields
    ), f"expected managed fields recorded at v1alpha1, got {managed_fields}"

    # Step 2: Server-side apply the same spec at v1. Reconciling the
    # v1alpha1-recorded managed fields sends partial objects through the
    # conversion webhook. Before the webhook tolerated partial objects this
    # failed with:
    #   failed to prune fields: failed add back owned items: failed to
    #   convert pruned object at version materialize.cloud/v1: conversion
    #   webhook for materialize.cloud/v1alpha1, Kind=Materialize failed
    mz_v1 = copy.deepcopy(definition["materialize"])
    mz_v1["apiVersion"] = "materialize.cloud/v1"
    for field in ("requestRollout", "inPlaceRollout", "environmentdIamRoleArn"):
        mz_v1["spec"].pop(field, None)
    apply_server_side(mz_v1)
    print("Server-side apply of the v1 resource succeeded")

    # Adopting the resource at v1 replaces the random initial requestRollout
    # with one derived from the v1 spec hash, which triggers one rollout.
    # Wait for it to complete so the assertions below aren't racing it.
    def check_rollout_complete():
        mz = get_materialize_v1alpha1()
        assert (
            mz["status"]["lastCompletedRolloutRequest"] == mz["spec"]["requestRollout"]
        ), f"Expected rollout to complete: requestRollout={mz['spec']['requestRollout']} != lastCompletedRolloutRequest={mz['status']['lastCompletedRolloutRequest']}"

    retry(check_rollout_complete, 600)
    mz = get_materialize_v1alpha1()
    adopted_request_rollout = mz["spec"]["requestRollout"]
    print(
        f"v1 adoption rollout completed: requestRollout changed from "
        f"{initial_request_rollout} to {adopted_request_rollout}"
    )

    # Step 3: Server-side re-apply the unchanged v1 spec (the routine GitOps
    # path). The derived requestRollout is deterministic, so this must not
    # trigger another rollout.
    apply_server_side(mz_v1)
    time.sleep(30)
    mz = get_materialize_v1alpha1()
    assert (
        mz["spec"]["requestRollout"] == adopted_request_rollout
    ), f"Expected requestRollout unchanged after server-side re-apply, but changed from {adopted_request_rollout} to {mz['spec']['requestRollout']}"
    print("Confirmed: no rollout triggered by unchanged server-side re-apply")

    # Step 4: Server-side apply a partial v1 manifest under a dedicated field
    # manager that owns only a subset of the spec, without forcing conflicts.
    # The value matches the existing one, so ownership becomes shared and
    # nothing changes.
    mz_partial = {
        "apiVersion": "materialize.cloud/v1",
        "kind": "Materialize",
        "metadata": {
            "name": mz_v1["metadata"]["name"],
            "namespace": mz_v1["metadata"]["namespace"],
        },
        "spec": {
            "environmentdImageRef": mz_v1["spec"]["environmentdImageRef"],
        },
    }
    apply_server_side(
        mz_partial, field_manager="materialize-ssa", force_conflicts=False
    )
    mz = get_materialize_v1alpha1()
    assert (
        mz["spec"]["requestRollout"] == adopted_request_rollout
    ), f"Expected requestRollout unchanged after partial server-side apply, but changed from {adopted_request_rollout} to {mz['spec']['requestRollout']}"
    print("Server-side apply of a partial v1 resource succeeded")

    # Step 5: Server-side apply a partial v1 manifest that does not contain
    # environmentdImageRef at all, under a field manager that does not own
    # it. The applied subset lacks the fields the conversion webhook used to
    # hard-require, and its owned subset stays that way on every future
    # apply. balancerdReplicas is excluded from the rollout hash and set to
    # its default, so nothing rolls out.
    #
    # This must force conflicts: orchestratord updates the Materialize
    # resource by writing the full serialized object, which spells every
    # unset optional spec field as an explicit null, so its field manager
    # ("unknown") owns all of them, including balancerdReplicas. Forcing only
    # transfers ownership of the one field this manifest sets.
    mz_no_image_ref = {
        "apiVersion": "materialize.cloud/v1",
        "kind": "Materialize",
        "metadata": {
            "name": mz_v1["metadata"]["name"],
            "namespace": mz_v1["metadata"]["namespace"],
        },
        "spec": {
            "balancerdReplicas": 2,
        },
    }
    apply_server_side(
        mz_no_image_ref, field_manager="materialize-ssa-subset", force_conflicts=True
    )
    managed_fields = json.loads(
        spawn.capture(
            [
                "kubectl",
                "get",
                "materializes.v1.materialize.cloud",
                mz_v1["metadata"]["name"],
                "-n",
                "materialize-environment",
                "--show-managed-fields",
                "-o",
                "jsonpath={.metadata.managedFields}",
            ]
        )
    )
    subset_entries = [
        entry
        for entry in managed_fields
        if entry["manager"] == "materialize-ssa-subset"
    ]
    assert len(subset_entries) == 1, f"expected one entry, got {managed_fields}"
    subset_fields = subset_entries[0]["fieldsV1"]["f:spec"]
    assert "f:balancerdReplicas" in subset_fields, f"got {subset_fields}"
    assert "f:environmentdImageRef" not in subset_fields, f"got {subset_fields}"

    mz = get_materialize_v1alpha1()
    assert (
        mz["spec"]["requestRollout"] == adopted_request_rollout
    ), f"Expected requestRollout unchanged after subset server-side apply, but changed from {adopted_request_rollout} to {mz['spec']['requestRollout']}"
    assert (
        mz["spec"]["environmentdImageRef"] == mz_v1["spec"]["environmentdImageRef"]
    ), f"Expected environmentdImageRef unchanged, got {mz['spec']['environmentdImageRef']}"

    # Re-apply the same subset. Its owned subset (which still lacks
    # environmentdImageRef) is now what gets round-tripped through the
    # conversion webhook during managed-field reconciliation.
    apply_server_side(
        mz_no_image_ref, field_manager="materialize-ssa-subset", force_conflicts=False
    )
    print(
        "Server-side apply of a partial v1 resource without "
        "environmentdImageRef succeeded"
    )

    # The resource must remain readable at both versions (both directions of
    # the conversion webhook).
    get_materialize_v1()
    get_materialize_v1alpha1()

    print("server-side apply test PASSED")


OPERATOR_CERT_SECRET = "operator-materialize-operator-cert"
OPERATOR_CA_SECRET = "operator-materialize-operator-ca"
MATERIALIZE_CRD = "materializes.materialize.cloud"


def conversion_webhook_works() -> bool:
    """Returns whether the conversion webhook is currently functional.

    Reading the Materialize resource at v1 forces the Kubernetes API
    server to call the conversion webhook to convert the stored v1alpha1
    object. If the webhook's serving certificate isn't trusted by the
    caBundle registered in the CRD, the API server rejects the call and this
    returns False.
    """
    result = subprocess.run(
        [
            "kubectl",
            "get",
            "materializes.v1.materialize.cloud",
            "-n",
            "materialize-environment",
            "-o",
            "json",
        ],
        capture_output=True,
    )
    if result.returncode != 0:
        print(
            f"conversion webhook probe failed: {result.stderr.decode(errors='replace')}"
        )
        return False
    items = json.loads(result.stdout).get("items", [])
    return len(items) > 0


def get_crd_ca_bundle() -> str:
    """The caBundle the API server uses to trust the conversion webhook."""
    return spawn.capture(
        [
            "kubectl",
            "get",
            "crd",
            MATERIALIZE_CRD,
            "-o",
            "jsonpath={.spec.conversion.webhook.clientConfig.caBundle}",
        ]
    ).strip()


def get_secret_field(secret: str, field: str) -> str:
    """A base64-encoded field from a secret in the materialize namespace."""
    return spawn.capture(
        [
            "kubectl",
            "get",
            "secret",
            secret,
            "-n",
            "materialize",
            "-o",
            rf"jsonpath={{.data.{field}}}",
        ]
    ).strip()


def get_serving_cert_ca() -> str:
    """The ca.crt in the mounted serving-certificate secret (the root CA)."""
    return get_secret_field(OPERATOR_CERT_SECRET, r"ca\.crt")


def get_serving_cert_leaf() -> str:
    """The tls.crt (leaf) in the mounted serving-certificate secret."""
    return get_secret_field(OPERATOR_CERT_SECRET, r"tls\.crt")


def workflow_webhook_cert_rotation(
    c: Composition,
    parser: WorkflowArgumentParser,
) -> None:
    """Test that the conversion webhook keeps working as its TLS certificate is
    rotated, i.e. once the original certificate would have expired.

    The webhook is served by orchestratord using a certificate that
    cert-manager rotates out-of-band, and orchestratord reloads the serving
    certificate from disk periodically. The serving certificate is signed by a
    stable root CA, so there are two distinct rotation cases, both tested here
    without restarting orchestratord:

      1. Serving-certificate rotation (the common case): the leaf rotates but
         the CA -- and therefore the caBundle registered on the CRD -- stays the
         same. The webhook must keep working with no caBundle change.

      2. Root CA rotation (rare): ca.crt changes, so orchestratord must
         re-register the CRD's caBundle to match the newly-served certificate,
         otherwise the API server would reject every conversion request.

    Rather than wait out real certificate lifetimes, this test deploys with a
    very short reload interval and forces cert-manager to reissue certificates
    by deleting their secrets.
    """
    parser.add_argument(
        "--recreate-cluster",
        action=argparse.BooleanOptionalAction,
        help="Recreate cluster if it exists already",
    )
    parser.add_argument(
        "--tag",
        type=str,
        help="Custom version tag to use",
    )
    parser.add_argument(
        "--orchestratord-override",
        default=True,
        action=argparse.BooleanOptionalAction,
        help="Override orchestratord tag",
    )
    args = parser.parse_args()

    definition = setup(c, args)
    enable_v1_crd(definition)

    # Use cert-manager (the default) so we exercise the real rotation path,
    # and reload the certificate aggressively so a rotation is picked up in
    # seconds rather than the default hour.
    assert definition["operator"]["operator"]["certificate"]["source"] == "cert-manager"
    definition["operator"]["operator"]["args"]["webhookCertReloadInterval"] = "5s"

    # Deploy a v1 resource. The initial apply already goes through the
    # conversion webhook (v1 -> stored v1alpha1), so this confirms the
    # webhook works before any rotation.
    definition["materialize"]["apiVersion"] = "materialize.cloud/v1"
    init(definition)
    apply_materialize(definition)

    def webhook_works() -> None:
        assert conversion_webhook_works(), "conversion webhook is not working"

    retry(webhook_works, 120)
    print("Conversion webhook works before rotation")

    initial_ca_bundle = get_crd_ca_bundle()
    assert initial_ca_bundle, "expected a caBundle to be registered on the CRD"

    # --- Case 1: serving-certificate rotation, CA unchanged ------------------
    #
    # Deleting the serving-certificate secret makes cert-manager reissue the
    # leaf (new key + new tls.crt) signed by the same stable root CA, so ca.crt
    # is unchanged. The webhook must keep working and the caBundle must NOT
    # change.
    leaf_before = get_serving_cert_leaf()
    ca_before = get_serving_cert_ca()
    print("Rotating the serving certificate (deleting the serving cert secret)...")
    spawn.runv(
        ["kubectl", "delete", "secret", OPERATOR_CERT_SECRET, "-n", "materialize"]
    )

    def leaf_rotated() -> None:
        leaf = get_serving_cert_leaf()
        assert (
            leaf and leaf != leaf_before
        ), "cert-manager has not reissued the leaf yet"

    retry(leaf_rotated, 120)
    assert (
        get_serving_cert_ca() == ca_before
    ), "root CA changed during a serving-certificate rotation; expected it to be stable"
    print("Serving certificate rotated; root CA is unchanged")

    # Give orchestratord time to reload the new leaf (interval is 5s), then
    # confirm the webhook still works and the caBundle was left untouched.
    retry(webhook_works, 120)
    assert (
        get_crd_ca_bundle() == initial_ca_bundle
    ), "caBundle changed even though the CA did not"
    apply_materialize(definition)
    print("Conversion webhook still works after serving-certificate rotation")

    # --- Case 2: root CA rotation --------------------------------------------
    #
    # Rotate the root CA, then reissue the leaf so its ca.crt reflects the new
    # CA. orchestratord must notice ca.crt changed and re-register the CRD's
    # caBundle, otherwise the API server would reject conversions.
    print("Rotating the root CA (deleting the CA secret)...")
    spawn.runv(["kubectl", "delete", "secret", OPERATOR_CA_SECRET, "-n", "materialize"])

    def ca_secret_rotated() -> None:
        ca = get_secret_field(OPERATOR_CA_SECRET, r"tls\.crt")
        assert ca, "cert-manager has not reissued the root CA yet"

    retry(ca_secret_rotated, 120)

    # Force the leaf to be re-signed by the new CA so the mounted ca.crt updates.
    spawn.runv(
        ["kubectl", "delete", "secret", OPERATOR_CERT_SECRET, "-n", "materialize"]
    )

    def serving_ca_changed() -> None:
        assert (
            get_serving_cert_ca() != ca_before
        ), "serving cert's ca.crt has not picked up the new root CA yet"

    retry(serving_ca_changed, 180)
    print("Root CA rotated and serving certificate re-signed by the new CA")

    # orchestratord must refresh the CRD's caBundle to match the new CA. Waiting
    # for the caBundle to change proves the re-registration happened.
    def ca_bundle_refreshed() -> None:
        current = get_crd_ca_bundle()
        assert (
            current and current != initial_ca_bundle
        ), "orchestratord has not refreshed the conversion webhook caBundle yet"

    retry(ca_bundle_refreshed, 300)
    print("orchestratord refreshed the conversion webhook caBundle after CA rotation")

    # Decisive check: the old CA is gone, so the webhook can only work if the
    # newly-served certificate is trusted via the refreshed caBundle. We never
    # restarted orchestratord, so this exercises the in-process reload +
    # re-registration path that runs in production.
    retry(webhook_works, 120)
    apply_materialize(definition)
    print("Conversion webhook still works after root CA rotation")
    print("webhook cert rotation test PASSED")


def workflow_manually_promote(
    c: Composition,
    parser: WorkflowArgumentParser,
) -> None:
    """Test ManuallyPromote rollout strategy with both v1alpha1 and v1
    force-promote mechanisms.

    Verifies that promotion can be triggered by setting forcePromote to either:
    - The v1alpha1 requestRollout UUID
    - The v1 requestedRolloutHash
    """
    parser.add_argument(
        "--recreate-cluster",
        action=argparse.BooleanOptionalAction,
        help="Recreate cluster if it exists already",
    )
    parser.add_argument(
        "--tag",
        type=str,
        help="Custom version tag to use",
    )
    parser.add_argument(
        "--orchestratord-override",
        default=True,
        action=argparse.BooleanOptionalAction,
        help="Override orchestratord tag",
    )
    args = parser.parse_args()

    definition = setup(c, args)
    enable_v1_crd(definition)

    # Deploy with v1 and ManuallyPromote strategy.
    definition["materialize"]["apiVersion"] = "materialize.cloud/v1"
    definition["materialize"]["spec"]["rolloutStrategy"] = "ManuallyPromote"
    init(definition)
    run(definition, False)
    print("Initial deployment with ManuallyPromote completed")

    # --- Test 1: Promote using v1alpha1 requestRollout UUID ---
    print("Test 1: Promote using v1alpha1 requestRollout UUID")

    # Make a spec change to trigger a new rollout.
    definition["materialize"]["spec"]["environmentdExtraEnv"] = [
        {"name": "MANUALLY_PROMOTE_TEST_1", "value": "true"},
    ]
    apply_materialize(definition)

    wait_for_ready_to_promote()

    # Promote using v1alpha1 requestRollout.
    mz_v1 = get_materialize_v1alpha1()
    request_rollout = mz_v1["spec"]["requestRollout"]
    mz_name = mz_v1["metadata"]["name"]
    print(f"Promoting via v1alpha1 requestRollout: {request_rollout}")
    spawn.runv(
        [
            "kubectl",
            "patch",
            "materializes.v1alpha1.materialize.cloud",
            mz_name,
            "-n",
            "materialize-environment",
            "--type=merge",
            "-p",
            json.dumps({"spec": {"forcePromote": request_rollout}}),
        ],
    )
    wait_for_rollout_complete()
    print("Test 1 PASSED: Promotion via v1alpha1 requestRollout succeeded")

    # --- Test 2: Promote using v1 requestedRolloutHash ---
    print("Test 2: Promote using v1 requestedRolloutHash")

    # Make another spec change to trigger a new rollout.
    definition["materialize"]["spec"]["environmentdExtraEnv"] = [
        {"name": "MANUALLY_PROMOTE_TEST_2", "value": "true"},
    ]
    apply_materialize(definition)

    wait_for_ready_to_promote()

    # Read the v1 status to get the requestedRolloutHash.
    mz_v2 = get_materialize_v1()
    requested_rollout_hash = mz_v2["status"]["requestedRolloutHash"]
    mz_name = mz_v2["metadata"]["name"]
    print(f"Promoting via v1 requestedRolloutHash: {requested_rollout_hash}")

    # Patch at v1 using the hash as forcePromote.
    spawn.runv(
        [
            "kubectl",
            "patch",
            "materializes.v1.materialize.cloud",
            mz_name,
            "-n",
            "materialize-environment",
            "--type=merge",
            "-p",
            json.dumps({"spec": {"forcePromote": requested_rollout_hash}}),
        ],
    )
    wait_for_rollout_complete()
    print("Test 2 PASSED: Promotion via v1 requestedRolloutHash succeeded")

    print("workflow_manually_promote PASSED")


def apply_materialize(definition: dict[str, Any]) -> None:
    """Apply the materialize resource definition via kubectl."""
    defs = [
        definition["namespace"],
        definition["secret"],
        definition["materialize"],
    ]
    if "materialize2" in definition:
        defs.append(definition["materialize2"])
    if "system_params_configmap" in definition:
        defs.append(definition["system_params_configmap"])
    yaml_str = yaml.dump_all(defs)
    print(f"Attempting to apply:\n{yaml_str}")
    transient_webhook_errors = (
        "connection refused",
        "deadline exceeded",
        "i/o timeout",
    )
    max_attempts = 120
    for attempt in range(max_attempts):
        result = subprocess.run(
            ["kubectl", "apply", "-f", "-"],
            input=yaml_str.encode(),
            capture_output=True,
        )
        if result.returncode == 0:
            break
        stderr_str = result.stderr.decode(errors="replace")
        if attempt < max_attempts - 1 and any(
            err in stderr_str for err in transient_webhook_errors
        ):
            print(f"Webhook not yet ready (attempt {attempt + 1}), retrying...")
            time.sleep(2)
            continue
        print(f"Failed to apply: {result.stdout}\nSTDERR:{result.stderr}")
        raise subprocess.CalledProcessError(
            result.returncode,
            result.args,
            output=result.stdout,
            stderr=result.stderr,
        )


def wait_for_ready_to_promote() -> None:
    """Wait for the Materialize resource to reach ReadyToPromote status."""
    for _ in range(900):
        time.sleep(1)
        if is_ready_to_manually_promote():
            break
    else:
        print(yaml.dump(get_materialize_at_stored_version()))
        raise RuntimeError("Never became ready for manual promotion")

    # Verify it stays in ReadyToPromote (doesn't auto-promote).
    time.sleep(30)
    if not is_ready_to_manually_promote():
        print(yaml.dump(get_materialize_at_stored_version()))
        raise RuntimeError("Stopped being ready for manual promotion before promoting")


def wait_for_rollout_complete() -> None:
    """Wait for the rollout to complete (UpToDate condition becomes True)."""
    for _ in range(900):
        time.sleep(1)
        try:
            status = get_materialize_status_at_stored_version()
            if not status:
                continue
            conditions = status.get("conditions", [])
            if (
                conditions
                and conditions[0]["type"] == "UpToDate"
                and conditions[0]["status"] == "True"
            ):
                return
        except subprocess.CalledProcessError:
            pass
    print(yaml.dump(get_materialize_at_stored_version()))
    raise RuntimeError("Rollout never completed")


def workflow_orchestratord_upgrade(
    c: Composition,
    parser: WorkflowArgumentParser,
) -> None:
    # TODO: ideally we'd just be able to compare the images directly, but
    # this test isn't consistently handling ghcr overrides yet - remove this
    # and go back to full image comparisons once we fix that
    def image_tag(image: str):
        return image.rsplit(":", 1)[1]

    def check_orchestratord_version(version: MzVersion):
        def check():
            data = get_orchestratord_data()
            assert len(data["items"]) == 1, f"got {len(data['items'])} items"

            got_image = data["items"][0]["spec"]["containers"][0]["image"]
            expected_image = get_image(
                c.compose["services"]["orchestratord"]["image"],
                str(version),
            )
            assert image_tag(got_image) == image_tag(
                expected_image
            ), f"{got_image} != {expected_image}"

        retry(check, 60)

    def check_environmentd_version(version: MzVersion):
        def check():
            data = get_environmentd_data()
            assert len(data["items"]) == 1, f"got {len(data['items'])} items"

            got_image = data["items"][0]["spec"]["containers"][0]["image"]
            expected_image = get_image(
                c.compose["services"]["environmentd"]["image"],
                str(version),
            )
            assert image_tag(got_image) == image_tag(
                expected_image
            ), f"{got_image} != {expected_image}"

        retry(check, 60)

    def check_clusterd_version(version: MzVersion):
        def check():
            data = get_clusterd_data()
            assert len(data["items"]) == 2, f"got {len(data['items'])} items"

            got_image = data["items"][0]["spec"]["containers"][0]["image"]
            expected_image = get_image(
                c.compose["services"]["clusterd"]["image"],
                str(version),
            )
            assert image_tag(got_image) == image_tag(
                expected_image
            ), f"{got_image} != {expected_image}"

        retry(check, 60)

    def check_balancerd_version(version: MzVersion):
        def check():
            data = get_balancerd_data()
            assert len(data["items"]) == 2, f"got {len(data['items'])} items"

            got_image = data["items"][0]["spec"]["containers"][0]["image"]
            expected_image = get_image(
                c.compose["services"]["balancerd"]["image"],
                str(version),
            )
            assert image_tag(got_image) == image_tag(
                expected_image
            ), f"{got_image} != {expected_image}"

        # balancerd pods have a 60s draining period after termination, so we
        # have to wait longer to ensure that the old ones are gone
        retry(check, 180)

    parser.add_argument(
        "--recreate-cluster",
        action=argparse.BooleanOptionalAction,
        help="Recreate cluster if it exists already",
    )
    parser.add_argument(
        "--tag",
        type=str,
        help="Custom version tag to use",
    )
    parser.add_argument(
        "--orchestratord-override",
        default=True,
        action=argparse.BooleanOptionalAction,
        help="Override orchestratord tag",
    )
    args = parser.parse_args()

    definition = setup(c, args)
    versions = get_all_self_managed_versions()
    versions.append(get_version(args.tag))

    def set_latest_supported_crd_version(definition: dict[str, Any]):
        if operator_supports_v1(definition):
            enable_v1_crd(definition)
            definition["materialize"]["apiVersion"] = "materialize.cloud/v1"
        else:
            definition["materialize"]["apiVersion"] = "materialize.cloud/v1alpha1"

    def request_rollout_if_needed(definition: dict[str, Any]):
        if definition["materialize"]["apiVersion"] == "materialize.cloud/v1alpha1":
            definition["materialize"]["spec"]["requestRollout"] = str(uuid.uuid4())
        else:
            definition["materialize"]["spec"].pop("requestRollout", None)

    print(f"running orchestratord {versions[-3]}")
    definition["operator"]["operator"]["image"]["tag"] = str(versions[-3])
    set_latest_supported_crd_version(definition)
    init(definition)
    check_orchestratord_version(versions[-3])

    print(f"running environmentd {versions[-3]}")
    definition["materialize"]["spec"]["environmentdImageRef"] = get_image(
        c.compose["services"]["environmentd"]["image"],
        str(versions[-3]),
    )
    run(definition, False)
    check_environmentd_version(versions[-3])
    check_clusterd_version(versions[-3])
    check_balancerd_version(versions[-3])

    for version in versions[-2:]:
        print(f"running orchestratord {version}")
        definition["operator"]["operator"]["image"]["tag"] = str(version)
        # Set before the helm upgrade so that operators which support the v1
        # CRD are deployed with --install-v1-crd and can serve the v1
        # apiVersion used below.
        set_latest_supported_crd_version(definition)
        helm_install_operator(definition["operator"], upgrade=True)
        wait_for_crd_established()
        check_crd_versions(definition)
        check_orchestratord_version(version)

        print(f"running environmentd {version}")
        definition["materialize"]["spec"]["environmentdImageRef"] = get_image(
            c.compose["services"]["environmentd"]["image"],
            str(version),
        )
        set_latest_supported_crd_version(definition)
        request_rollout_if_needed(definition)
        run(definition, False)
        check_environmentd_version(version)
        check_clusterd_version(version)
        # balancerd is broken in the upgrade to 26.4.0
        if str(version) != "v26.4.0":
            check_balancerd_version(version)

    # We cannot roll back orchestratord versions once the CRD is updated,
    # so let's just get a clean cluster and start over.
    spawn.runv(
        [
            "kind",
            "delete",
            "cluster",
            "--name",
            "kind",
        ]
    )
    definition = setup(c, args)

    print(f"running orchestratord {versions[-3]}")
    definition["operator"]["operator"]["image"]["tag"] = str(versions[-3])
    set_latest_supported_crd_version(definition)
    init(definition)
    check_orchestratord_version(versions[-3])

    print(f"running environmentd {versions[-3]}")
    definition["materialize"]["spec"]["environmentdImageRef"] = get_image(
        c.compose["services"]["environmentd"]["image"],
        str(versions[-3]),
    )
    run(definition, False)
    check_environmentd_version(versions[-3])
    check_clusterd_version(versions[-3])
    check_balancerd_version(versions[-3])

    print(f"running orchestratord {versions[-1]}")
    definition["operator"]["operator"]["image"]["tag"] = str(versions[-1])
    # Set before the helm upgrade so that operators which support the v1 CRD
    # are deployed with --install-v1-crd and can serve the v1 apiVersion used
    # below.
    set_latest_supported_crd_version(definition)
    helm_install_operator(definition["operator"], upgrade=True)
    wait_for_crd_established()
    check_crd_versions(definition)
    check_orchestratord_version(versions[-1])

    print(f"running environmentd {versions[-1]}")
    definition["materialize"]["spec"]["environmentdImageRef"] = get_image(
        c.compose["services"]["environmentd"]["image"],
        str(versions[-1]),
    )
    set_latest_supported_crd_version(definition)
    request_rollout_if_needed(definition)
    run(definition, False)
    check_environmentd_version(versions[-1])
    check_clusterd_version(versions[-1])
    # balancerd is broken in the upgrade to 26.4.0
    if str(versions[-1]) != "v26.4.0":
        check_balancerd_version(versions[-1])


def workflow_revert_rollout(c: Composition, parser: WorkflowArgumentParser) -> None:
    # Regression test for DEP-42: if a user starts an upgrade and then cancels
    # by reverting only `spec.requestRollout` without also reverting
    # `spec.environmentdImageRef`, the spec image stays ahead of the image
    # actually running in environmentd. Downstream resources (balancerd,
    # console) must continue tracking the last completed rollout's image
    # rather than the diverged spec image — otherwise they end up
    # version-skewed from environmentd.
    #
    # We exercise this end-to-end by initial-deploying on a prior released
    # version, then starting a `ManuallyPromote` upgrade to the current
    # build's image and parking it at `ReadyToPromote` (never promoting it),
    # and finally reverting only `requestRollout`.
    parser.add_argument(
        "--recreate-cluster",
        action=argparse.BooleanOptionalAction,
        help="Recreate cluster if it exists already",
    )
    parser.add_argument("--tag", type=str, help="Custom version tag to use")
    parser.add_argument(
        "--orchestratord-override",
        default=True,
        action=argparse.BooleanOptionalAction,
        help="Override orchestratord tag",
    )
    args = parser.parse_args()

    def get_cr(plural: str) -> dict[str, Any]:
        # orchestratord creates the Balancer and Console CRs after writing
        # `lastCompletedRolloutRequest`, so `post_run_check` can return
        # before they exist. Retry until the resource shows up.
        #
        # Use this only for the Balancer/Console CRs. The Materialize CR must
        # be read via `get_materialize_at_stored_version`, since a bare
        # `materializes` resolves to the default-served v1 version, which
        # lacks `requestRollout`/`lastCompletedRolloutRequest`.
        result: dict[str, Any] = {}

        def fetch() -> None:
            data = json.loads(
                spawn.capture(
                    [
                        "kubectl",
                        "get",
                        plural,
                        "-n",
                        "materialize-environment",
                        "-o",
                        "json",
                    ]
                )
            )
            assert len(data["items"]) >= 1, f"{plural} not yet present"
            result["item"] = data["items"][0]

        retry(fetch, 120)
        return result["item"]

    definition = setup(c, args)

    # Initial deploy on a prior released version, so the upgrade target
    # (current build) is a real, pullable, different image. orchestratord
    # itself stays on the current build — that's the binary whose fix is
    # under test.
    initial_version = get_all_self_managed_versions()[-1]
    initial_image = get_image(
        c.compose["services"]["environmentd"]["image"],
        str(initial_version),
    )
    upgrade_image = get_image(
        c.compose["services"]["environmentd"]["image"],
        args.tag,
    )
    assert upgrade_image != initial_image, (
        "test setup invariant: initial and upgrade env images must differ "
        f"(both are {initial_image!r})"
    )

    definition["materialize"]["spec"]["environmentdImageRef"] = initial_image
    init(definition)
    run(definition, False)

    initial_mz = get_materialize_at_stored_version()
    initial_request = initial_mz["spec"]["requestRollout"]
    assert initial_mz["status"]["lastCompletedRolloutRequest"] == initial_request
    assert (
        initial_mz["status"]["lastCompletedRolloutEnvironmentdImageRef"]
        == initial_image
    ), (
        "status.lastCompletedRolloutEnvironmentdImageRef was not populated "
        "with the rolled-out image"
    )
    initial_balancerd_image_ref = get_cr("balancers")["spec"]["balancerdImageRef"]
    initial_console_image_ref = get_cr("consoles")["spec"]["consoleImageRef"]

    # Kick off a real ManuallyPromote upgrade to the current build's image
    # and leave it parked at ReadyToPromote. At this point the new-generation
    # environmentd is up and running, but the active environmentd is still
    # the initial-version pod, so `status.lastCompletedRollout*` must not
    # have advanced.
    definition["materialize"]["spec"]["rolloutStrategy"] = "ManuallyPromote"
    definition["materialize"]["spec"]["environmentdImageRef"] = upgrade_image
    definition["materialize"]["spec"]["requestRollout"] = str(uuid.uuid4())
    spawn.runv(
        ["kubectl", "apply", "-f", "-"],
        stdin=yaml.dump_all(
            [
                definition["namespace"],
                definition["secret"],
                definition["materialize"],
            ]
        ).encode(),
    )
    for _ in range(900):
        time.sleep(1)
        if is_ready_to_manually_promote():
            break
    else:
        spawn.runv(
            [
                "kubectl",
                "get",
                "materializes",
                "-n",
                "materialize-environment",
                "-o",
                "yaml",
            ],
        )
        raise RuntimeError("upgrade never became ready for manual promotion")

    parked_mz = get_materialize_at_stored_version()
    assert parked_mz["status"]["lastCompletedRolloutRequest"] == initial_request
    assert (
        parked_mz["status"]["lastCompletedRolloutEnvironmentdImageRef"] == initial_image
    )

    # Even mid-rollout (spec.envImageRef = upgrade, status = initial),
    # downstream CRs must track the active image, not the spec image.
    parked_balancerd_image_ref = get_cr("balancers")["spec"]["balancerdImageRef"]
    assert parked_balancerd_image_ref == initial_balancerd_image_ref, (
        f"Balancer CR balancerdImageRef drifted during a parked upgrade: "
        f"{initial_balancerd_image_ref!r} -> {parked_balancerd_image_ref!r}"
    )
    parked_console_image_ref = get_cr("consoles")["spec"]["consoleImageRef"]
    assert parked_console_image_ref == initial_console_image_ref, (
        f"Console CR consoleImageRef drifted during a parked upgrade: "
        f"{initial_console_image_ref!r} -> {parked_console_image_ref!r}"
    )

    # Cancel the upgrade by reverting only `requestRollout`. Deliberately
    # leave `environmentdImageRef` pointed at the upgrade image — that's the
    # DEP-42 scenario: spec image and running image diverge.
    definition["materialize"]["spec"]["requestRollout"] = initial_request
    spawn.runv(
        ["kubectl", "apply", "-f", "-"],
        stdin=yaml.dump(definition["materialize"]).encode(),
    )

    def check_revert_applied() -> None:
        mz = get_materialize_at_stored_version()
        assert mz["spec"]["requestRollout"] == initial_request
        assert mz["spec"]["environmentdImageRef"] == upgrade_image

    retry(check_revert_applied, 60)

    # Let orchestratord reconcile several times after the revert.
    time.sleep(30)

    final_mz = get_materialize_at_stored_version()
    assert final_mz["status"]["lastCompletedRolloutRequest"] == initial_request
    assert (
        final_mz["status"]["lastCompletedRolloutEnvironmentdImageRef"] == initial_image
    ), "lastCompletedRolloutEnvironmentdImageRef should not change without a completed rollout"

    final_balancerd_image_ref = get_cr("balancers")["spec"]["balancerdImageRef"]
    assert final_balancerd_image_ref == initial_balancerd_image_ref, (
        f"Balancer CR balancerdImageRef tracked diverged spec image instead "
        f"of the last completed rollout image: "
        f"{initial_balancerd_image_ref!r} -> {final_balancerd_image_ref!r}"
    )

    final_console_image_ref = get_cr("consoles")["spec"]["consoleImageRef"]
    assert final_console_image_ref == initial_console_image_ref, (
        f"Console CR consoleImageRef tracked diverged spec image instead of "
        f"the last completed rollout image: "
        f"{initial_console_image_ref!r} -> {final_console_image_ref!r}"
    )


def workflow_rollout_timeout(c: Composition, parser: WorkflowArgumentParser) -> None:
    # Tests CLO-81: orchestratord automatically cancels an in-progress rollout
    # once it has been running longer than `spec.rolloutRequestTimeout`. A new
    # generation left un-promoted holds back compaction via read holds, and
    # promoting it after a long delay can cause incident-inducing load, so
    # instead of parking it indefinitely orchestratord tears it down and keeps
    # serving the previously-active generation. The user must request a fresh
    # rollout to retry.
    #
    # We deploy on a prior released version, then start a default
    # (WaitUntilReady) upgrade to the current build with a deliberately tiny
    # `rolloutRequestTimeout`. The new generation cannot boot and catch up
    # within the timeout, so orchestratord must cancel the rollout: the active
    # environmentd stays on the initial image, the un-promoted generation is
    # torn down, `status.conditions` reports reason `RolloutTimeout`, and
    # `lastCompletedRolloutRequest` advances to the cancelled request (so it is
    # not immediately retried) while `lastCompletedRolloutEnvironmentdImageRef`
    # never moves to the upgrade image.
    parser.add_argument(
        "--recreate-cluster",
        action=argparse.BooleanOptionalAction,
        help="Recreate cluster if it exists already",
    )
    parser.add_argument("--tag", type=str, help="Custom version tag to use")
    parser.add_argument(
        "--orchestratord-override",
        default=True,
        action=argparse.BooleanOptionalAction,
        help="Override orchestratord tag",
    )
    args = parser.parse_args()

    definition = setup(c, args)

    # Initial deploy on a prior released version so the upgrade target (current
    # build) is a real, pullable, different image. orchestratord stays on the
    # current build — that's the binary whose timeout logic is under test.
    initial_version = get_all_self_managed_versions()[-1]
    initial_image = get_image(
        c.compose["services"]["environmentd"]["image"],
        str(initial_version),
    )
    upgrade_image = get_image(
        c.compose["services"]["environmentd"]["image"],
        args.tag,
    )
    assert upgrade_image != initial_image, (
        "test setup invariant: initial and upgrade env images must differ "
        f"(both are {initial_image!r})"
    )

    definition["materialize"]["spec"]["environmentdImageRef"] = initial_image
    init(definition)
    run(definition, False)

    initial_status = get_materialize_status_at_stored_version()
    assert initial_status is not None
    assert initial_status["lastCompletedRolloutEnvironmentdImageRef"] == initial_image

    # Start a default (WaitUntilReady) upgrade with a tiny timeout. The new
    # generation cannot become ready within the timeout, so the rollout will be
    # cancelled. We apply the manifest directly (rather than via `run`, which
    # would wait for a successful rollout) and poll for the cancellation.
    upgrade_request = str(uuid.uuid4())
    definition["materialize"]["spec"]["environmentdImageRef"] = upgrade_image
    definition["materialize"]["spec"]["rolloutRequestTimeout"] = "1s"
    definition["materialize"]["spec"]["requestRollout"] = upgrade_request
    spawn.runv(
        ["kubectl", "apply", "-f", "-"],
        stdin=yaml.dump_all(
            [
                definition["namespace"],
                definition["secret"],
                definition["materialize"],
            ]
        ).encode(),
    )

    # Wait for orchestratord to observe the timeout and cancel the rollout.
    def check_cancelled() -> None:
        status = get_materialize_status_at_stored_version() or {}
        conditions = status.get("conditions") or []
        assert conditions, "no status conditions yet"
        condition = conditions[0]
        assert (
            condition["type"] == "UpToDate"
            and condition["status"] == "False"
            and condition["reason"] == "RolloutTimeout"
        ), f"expected a RolloutTimeout condition, but got {condition}"
        # The cancelled request is marked completed so it isn't retried...
        assert (
            status["lastCompletedRolloutRequest"] == upgrade_request
        ), f"lastCompletedRolloutRequest did not advance to the cancelled request: {status}"
        # ...but the rollout never actually became active.
        assert (
            status["lastCompletedRolloutEnvironmentdImageRef"] == initial_image
        ), f"lastCompletedRolloutEnvironmentdImageRef advanced despite cancellation: {status}"

    retry(check_cancelled, 300)

    # The previously-active generation keeps serving on the initial image, and
    # the un-promoted generation has been torn down (releasing its read holds).
    def check_active_generation_intact() -> None:
        live_images = [
            item["spec"]["containers"][0]["image"]
            for item in get_environmentd_data()["items"]
            if not item.get("metadata", {}).get("deletionTimestamp")
        ]
        assert live_images, "no environmentd pods running"
        assert all(
            image != upgrade_image for image in live_images
        ), f"cancelled upgrade generation is still running: {live_images}"
        assert any(
            image == initial_image for image in live_images
        ), f"active generation is not on the initial image: {live_images}"

    retry(check_active_generation_intact, 120)

    def check_single_generation() -> None:
        statefulsets = (
            spawn.capture(
                [
                    "kubectl",
                    "get",
                    "statefulset",
                    "-n",
                    "materialize-environment",
                    "-o",
                    "name",
                ],
                stderr=subprocess.DEVNULL,
            )
            .strip()
            .split("\n")
        )
        environmentd_statefulsets = [s for s in statefulsets if "-environmentd-" in s]
        assert (
            len(environmentd_statefulsets) == 1
        ), f"expected exactly one environmentd statefulset after cancellation, but found {environmentd_statefulsets}"

    retry(check_single_generation, 120)


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    parser.add_argument(
        "--recreate-cluster",
        action=argparse.BooleanOptionalAction,
        help="Recreate cluster if it exists already",
    )
    parser.add_argument(
        "--tag",
        type=str,
        help="Custom version tag to use",
    )
    parser.add_argument(
        "--orchestratord-override",
        default=True,
        action=argparse.BooleanOptionalAction,
        help="Override orchestratord tag",
    )
    parser.add_argument("--seed", type=str, default=random.randrange(1000000))
    parser.add_argument("--scenario", type=str)
    parser.add_argument(
        "--action", type=str, default="noop", choices=[elem.value for elem in Action]
    )
    parser.add_argument(
        "--properties",
        type=str,
        default="individual",
        choices=[elem.value for elem in Properties],
    )
    parser.add_argument("--modification", action="append", type=str, default=[])
    parser.add_argument("--runtime", type=int, help="Runtime in seconds")
    args = parser.parse_args()

    print(f"--- Random seed is {args.seed}")

    definition = setup(c, args)

    rng = random.Random(args.seed)

    mod_classes = sorted(all_modifications(), key=repr)
    if args.modification:
        mod_classes = [
            mod_class
            for mod_class in mod_classes
            if mod_class.__name__ in args.modification
        ]
    mod_classes = buildkite.shard_list(mod_classes, lambda mod: mod.__name__)

    if args.scenario:
        assert not args.runtime
        assert not args.modification
        run_scenario(
            [
                [Modification.from_dict(mod) for mod in mods]
                for mods in json.loads(args.scenario)
            ],
            definition,
        )
        return

    if args.runtime:
        end_time = (
            datetime.datetime.now() + datetime.timedelta(seconds=args.runtime)
        ).timestamp()

    action = Action(args.action)
    properties = Properties(args.properties)
    mod_source = make_mod_source(properties, mod_classes, rng)

    end_time = (
        datetime.datetime.now() + datetime.timedelta(seconds=args.runtime or 1800)
    ).timestamp()
    versions = get_all_self_managed_versions()

    try:
        while time.time() < end_time:
            if action == Action.Noop:
                mods = mod_source.next_mods(get_version(args.tag))
                if args.tag:
                    mods.append(EnvironmentdImageRef(str(args.tag)))
                run_scenario([mods], definition)

            elif action == Action.Upgrade:
                current_version = rng.choice(versions[:-1])
                target_version = get_upgrade_target(rng, current_version, versions)
                mods = mod_source.next_mods(current_version)
                scenario = [
                    [EnvironmentdImageRef(str(v))] + mods
                    for v in (current_version, target_version)
                ]
                run_scenario(scenario, definition)

            elif action == Action.UpgradeChain:
                max_chain_length = 6
                current_version = rng.choice(versions)
                chain = [current_version]
                next_version = current_version

                try:
                    for _ in range(len(versions)):
                        if len(chain) >= max_chain_length:
                            break
                        next_version = get_upgrade_target(rng, next_version, versions)
                        chain.append(next_version)
                except ValueError:
                    # We can't upgrade any further, just run the test as far as it goes now
                    pass

                mods = mod_source.next_mods(current_version)
                scenario = [
                    [EnvironmentdImageRef(str(version))] + mods for version in chain
                ]
                run_scenario(scenario, definition)

            else:
                raise ValueError(f"Unhandled action {action}")
    except StopIteration:
        pass


def setup(c: Composition, args) -> dict[str, Any]:
    c.up(Service("testdrive", idle=True), Service("mz-debug", idle=True))
    c.invoke("cp", "mz-debug:/usr/local/bin/mz-debug", ".")

    cluster = KIND_CLUSTER_NAME
    clusters = spawn.capture(["kind", "get", "clusters"]).strip().split("\n")
    if cluster not in clusters or args.recreate_cluster:
        recreate_kind_cluster()

        helm_install_cert_manager()

        spawn.runv(["kubectl", "create", "namespace", "materialize"])

        spawn.runv(
            [
                "kubectl",
                "apply",
                "-f",
                MZ_ROOT / "misc" / "helm-charts" / "testing" / "postgres.yaml",
            ]
        )
        spawn.runv(
            [
                "kubectl",
                "apply",
                "-f",
                MZ_ROOT / "misc" / "helm-charts" / "testing" / "minio.yaml",
            ]
        )
        spawn.runv(
            [
                "kubectl",
                "apply",
                "-f",
                MZ_ROOT / "test" / "orchestratord" / "storageclass.yaml",
            ]
        )

    if not args.tag:
        services = [
            "orchestratord",
            "environmentd",
            "clusterd",
            "balancerd",
        ]
        c.pull_images(*services)
        for service in services:
            spawn.runv(
                [
                    "docker",
                    "tag",
                    c.compose["services"][service]["image"],
                    get_image(c.compose["services"][service]["image"], None),
                ]
            )
        spawn.runv(
            ["kind", "load", "docker-image", "--name", cluster]
            + [
                get_image(c.compose["services"][service]["image"], None)
                for service in services
            ]
        )

    definition: dict[str, Any] = {}

    with open(MZ_ROOT / "misc" / "helm-charts" / "operator" / "values.yaml") as f:
        definition["operator"] = yaml.load(f, Loader=yaml.Loader)
    with open(MZ_ROOT / "misc" / "helm-charts" / "testing" / "materialize.yaml") as f:
        materialize_setup = list(yaml.load_all(f, Loader=yaml.Loader))
        assert len(materialize_setup) == 3
        definition["namespace"] = materialize_setup[0]
        definition["secret"] = materialize_setup[1]
        definition["materialize"] = materialize_setup[2]
    with open(MZ_ROOT / "misc" / "helm-charts" / "testing" / "balancer.yaml") as f:
        definition["balancer"] = yaml.load(f, Loader=yaml.Loader)

    get_version(args.tag)
    if args.orchestratord_override:
        definition["operator"]["operator"]["image"]["tag"] = get_tag(args.tag)
    # TODO: database-issues#9696, makes environmentd -> clusterd connections fail
    # definition["operator"]["networkPolicies"]["enabled"] = True
    # definition["operator"]["networkPolicies"]["internal"]["enabled"] = True
    # definition["operator"]["networkPolicies"]["egress"]["enabled"] = True
    # definition["operator"]["networkPolicies"]["ingress"]["enabled"] = True
    # TODO: Remove when fixed: error: unexpected argument '--disable-license-key-checks' found
    definition["operator"]["operator"]["args"]["enableLicenseKeyChecks"] = True
    definition["operator"]["clusterd"]["nodeSelector"][
        "workload"
    ] = "materialize-instance"
    definition["operator"]["environmentd"]["nodeSelector"][
        "workload"
    ] = "materialize-instance"
    definition["secret"]["stringData"]["license_key"] = os.environ["MZ_CI_LICENSE_KEY"]
    definition["materialize"]["spec"]["environmentdImageRef"] = get_image(
        c.compose["services"]["environmentd"]["image"], args.tag
    )
    definition["balancer"]["spec"]["balancerdImageRef"] = get_image(
        c.compose["services"]["balancerd"]["image"], args.tag
    )
    # this is just a hack to get balancerd to start up, sending requests
    # won't actually work
    definition["balancer"]["spec"]["staticRouting"][
        "environmentdNamespace"
    ] = "materialize"
    definition["balancer"]["spec"]["staticRouting"][
        "environmentdServiceName"
    ] = "postgres"
    # kubectl get endpoints mzel5y3f42l6-cluster-u1-replica-u1-gen-1 -n materialize-environment -o json
    # more than one address
    return definition


DONE_SCENARIOS = set()


def run_scenario(
    scenario: list[list[Modification]],
    original_definition: dict[str, Any],
    modify: bool = True,
) -> None:
    initialize = True
    scenario_json = json.dumps([[mod.to_dict() for mod in mods] for mods in scenario])
    if scenario_json in DONE_SCENARIOS:
        return
    DONE_SCENARIOS.add(scenario_json)
    print(f"--- Running with {scenario_json}")
    for mods in scenario:
        definition = copy.deepcopy(original_definition)
        expect_fail = False
        if modify:
            for mod in mods:
                mod.modify(definition)
                if mod.value in mod.failed_reconciliation_values():
                    expect_fail = True
        if initialize:
            init(definition)
            run(definition, expect_fail)
            initialize = False  # only initialize once
        else:
            helm_install_operator(
                values=definition["operator"],
                upgrade=True,
            )
            # The set of served CRD versions may change between steps (e.g.
            # when installV1CRD flips), so wait until the operator has
            # re-registered the CRD before applying resources against it.
            wait_for_crd_established()
            check_crd_versions(definition)
            if definition["materialize"]["apiVersion"] == "materialize.cloud/v1alpha1":
                definition["materialize"]["spec"]["requestRollout"] = str(uuid.uuid4())
            run(definition, expect_fail)
        mod_dict = {mod.__class__: mod.value for mod in mods}
        for subclass in all_subclasses(Modification):
            if subclass not in mod_dict:
                mod_dict[subclass] = subclass.default()
        try:
            if not expect_fail:
                for mod in mods:
                    mod.validate(mod_dict)
        except:
            print(
                f"Reproduce with bin/mzcompose --find orchestratord run default --recreate-cluster --scenario='{scenario_json}'"
            )
            raise
        finally:
            if not expect_fail:
                run_mz_debug()


def is_prerelease_tag(tag: str) -> bool:
    version = Version.parse(tag.removeprefix("v"))
    if version.prerelease is not None:
        return True
    if version.build is not None:
        return True
    return False


def helm_install_operator(
    values: dict[str, Any],
    upgrade: bool,
):
    tag = values.get("operator", {}).get("image", {}).get("tag")
    helm_release_version = tag or "dev"
    chart_path = MZ_ROOT / "misc" / "helm-charts" / "operator"

    # If installing existing released versions of orchestratord,
    # we should use the corresponding helm chart.
    if not is_prerelease_tag(tag or "v26.0.0"):
        chart_path = "materialize/materialize-operator"
        spawn.runv(
            [
                "helm",
                "repo",
                "add",
                "materialize",
                "https://materializeinc.github.io/materialize",
            ]
        )
        spawn.runv(["helm", "repo", "update", "materialize"])

    operation = "upgrade" if upgrade else "install"
    stdin = yaml.dump(values).encode()

    try:
        spawn.runv(
            [
                "helm",
                operation,
                "operator",
                chart_path,
                "--namespace=materialize",
                "--create-namespace",
                "--version",
                helm_release_version,
                "--wait",
                "-f",
                "-",
            ],
            stdin=stdin,
        )
    except subprocess.CalledProcessError:
        # Helm's --wait can panic due to a race condition in fluxcd/cli-utils
        # (send on closed channel). On failure, clean up the potentially
        # broken release and retry once.
        print("helm install failed, retrying...")
        try:
            spawn.capture(
                ["helm", "uninstall", "operator", "--namespace=materialize"],
                stderr=subprocess.DEVNULL,
            )
        except subprocess.CalledProcessError:
            pass
        spawn.runv(
            [
                "helm",
                "install",
                "operator",
                chart_path,
                "--namespace=materialize",
                "--create-namespace",
                "--version",
                helm_release_version,
                "--wait",
                "-f",
                "-",
            ],
            stdin=stdin,
        )


def helm_install_cert_manager():
    spawn.runv(
        [
            "helm",
            "install",
            "cert-manager",
            "oci://quay.io/jetstack/charts/cert-manager",
            "--version",
            "v1.19.2",
            "--namespace",
            "cert-manager",
            "--create-namespace",
            "--set",
            "crds.enabled=true",
        ]
    )


def init(definition: dict[str, Any]) -> None:
    # `--wait=true` blocks until the namespace is fully terminated. If the
    # timeout is hit and we proceed anyway, the next `kubectl apply` races the
    # terminating namespace and fails with "is being terminated", so let any
    # timeout error propagate instead of swallowing it.
    spawn.runv(
        [
            "kubectl",
            "delete",
            "namespace",
            "materialize-environment",
            "--ignore-not-found",
            "--wait=true",
            "--timeout=300s",
        ],
    )
    try:
        spawn.capture(
            ["helm", "uninstall", "operator", "--namespace=materialize"],
            stderr=subprocess.DEVNULL,
        )
    except subprocess.CalledProcessError:
        pass

    helm_install_operator(
        values=definition["operator"],
        upgrade=False,
    )

    wait_for_crd_established()
    check_crd_versions(definition)


def check_crd_versions(definition: dict[str, Any]) -> None:
    """Check that the v1 CRD version and the conversion webhook are installed
    if and only if the operator was asked to install them."""

    def check() -> None:
        crd = json.loads(
            spawn.capture(
                ["kubectl", "get", "crd", MATERIALIZE_CRD, "-o", "json"],
                stderr=subprocess.DEVNULL,
            )
        )
        versions = sorted(version["name"] for version in crd["spec"]["versions"])
        conversion_strategy = (crd["spec"].get("conversion") or {}).get("strategy")
        if operator_serves_v1(definition):
            assert versions == [
                "v1",
                "v1alpha1",
            ], f"expected v1 and v1alpha1 to be served, got {versions}"
            assert (
                conversion_strategy == "Webhook"
            ), f"expected Webhook conversion, got {conversion_strategy}"
        else:
            assert versions == [
                "v1alpha1"
            ], f"expected only v1alpha1 to be served, got {versions}"
            # The API server defaults spec.conversion to {"strategy": "None"}
            # (the string "None") when no conversion is configured.
            assert conversion_strategy in (
                None,
                "None",
            ), f"expected no conversion, got {conversion_strategy}"

    retry(check, 240)


def wait_for_crd_established():
    for _ in range(240):
        try:
            crd = json.loads(
                spawn.capture(
                    [
                        "kubectl",
                        "get",
                        "crd",
                        "materializes.materialize.cloud",
                        "-n",
                        "materialize",
                        "-o",
                        "json",
                    ],
                    stderr=subprocess.DEVNULL,
                )
            )
            conditions = crd.get("status", {}).get("conditions") or []
            established_condition = next(
                (
                    condition
                    for condition in conditions
                    if condition["type"] == "Established"
                ),
                {"status": "False"},
            )
            if established_condition["status"] == "True":
                break

        except subprocess.CalledProcessError:
            pass
        time.sleep(1)
    else:
        raise ValueError("CRD never became 'Established'")


def run(definition: dict[str, Any], expect_fail: bool) -> None:
    apply_materialize(definition)

    if definition["materialize"]["spec"].get("rolloutStrategy") == "ManuallyPromote":
        wait_for_ready_to_promote()

        # Manually promote it by reading the v1alpha1 resource to get the
        # requestRollout UUID, then patching forcePromote to match it.
        # Alternatively, forcePromote can be set to the v1
        # requestedRolloutHash (tested in workflow_manually_promote).
        mz = get_materialize_v1alpha1()
        request_rollout = mz["spec"]["requestRollout"]
        assert request_rollout is not None
        mz_name = mz["metadata"]["name"]
        try:
            spawn.runv(
                [
                    "kubectl",
                    "patch",
                    "materializes.v1alpha1.materialize.cloud",
                    mz_name,
                    "-n",
                    "materialize-environment",
                    "--type=merge",
                    "-p",
                    json.dumps({"spec": {"forcePromote": request_rollout}}),
                ],
            )
        except subprocess.CalledProcessError as e:
            print(f"Failed to apply: {e.stdout}\nSTDERR:{e.stderr}")
            raise

    post_run_check(definition, expect_fail)


def is_ready_to_manually_promote():
    mz = get_materialize_at_stored_version()
    conditions = mz.get("status", {}).get("conditions")
    return (
        conditions is not None
        and len(conditions)
        and conditions[0]["type"] == "UpToDate"
        and conditions[0]["status"] == "Unknown"
        and conditions[0]["reason"] == "ReadyToPromote"
    )


def post_run_check(definition: dict[str, Any], expect_fail: bool) -> None:
    # Read at the stored version explicitly to avoid going through the
    # conversion webhook, which may not be ready yet during initial deployment.
    for i in range(900):
        time.sleep(1)
        try:
            mz = get_materialize_at_stored_version()
            status = mz.get("status")
            if not status:
                continue
            if expect_fail:
                break
            if (
                not status["conditions"]
                or status["conditions"][0]["type"] != "UpToDate"
                or status["conditions"][0]["status"] != "True"
            ):
                continue
            # Wait for the rollout of the spec we just applied to complete,
            # not a stale rollout left over from a previous upgrade step. On
            # an upgrade the operator may still report the old rollout as
            # UpToDate before it reacts to the new spec, and returning early
            # there races the `ImmediatelyPromoteCausingDowntime` teardown of
            # the old environmentd pod (leaving validate with zero pods).
            # Both v1 and v1alpha1 resources are stored as v1alpha1, where the
            # conversion webhook derives `spec.requestRollout` from the v1 spec
            # hash, so comparing requestRollout UUIDs works for either
            # apiVersion.
            last_completed = status.get("lastCompletedRolloutRequest")
            if last_completed is not None and last_completed == mz["spec"].get(
                "requestRollout"
            ):
                break
        except subprocess.CalledProcessError:
            pass
    else:
        spawn.runv(
            [
                "kubectl",
                "get",
                "materializes.v1alpha1.materialize.cloud",
                "-n",
                "materialize-environment",
                "-o",
                "yaml",
            ],
        )
        raise ValueError("Never completed")

    for i in range(480):
        try:
            status = spawn.capture(
                [
                    "kubectl",
                    "get",
                    "pods",
                    "-l",
                    "app=environmentd",
                    "-n",
                    "materialize-environment",
                    "-o",
                    "jsonpath={.items[0].status.phase}",
                ],
                stderr=subprocess.DEVNULL,
            )
            if status in ["Running", "Error", "CrashLoopBackOff"]:
                assert not expect_fail
                break
        except subprocess.CalledProcessError:
            if expect_fail:
                try:
                    logs = spawn.capture(
                        [
                            "kubectl",
                            "logs",
                            "-l",
                            "app.kubernetes.io/instance=operator",
                            "-n",
                            "materialize",
                        ],
                        stderr=subprocess.DEVNULL,
                    )
                    if (
                        f"ERROR reconciling object{{object.ref=Materialize.v1alpha1.materialize.cloud/{definition['materialize']['metadata']['name']}.materialize-environment"
                        in logs
                    ):
                        break
                except subprocess.CalledProcessError:
                    pass

        time.sleep(1)
    else:
        # Helps to debug
        spawn.runv(
            [
                "kubectl",
                "describe",
                "pod",
                "-l",
                "app=environmentd",
                "-n",
                "materialize-environment",
            ]
        )
        raise ValueError("Never completed")


def run_balancer(definition: dict[str, Any], expect_fail: bool) -> None:
    defs = [
        definition["namespace"],
        definition["balancer"],
    ]
    try:
        spawn.runv(
            ["kubectl", "apply", "-f", "-"],
            stdin=yaml.dump_all(defs).encode(),
        )
    except subprocess.CalledProcessError as e:
        print(f"Failed to apply: {e.stdout}\nSTDERR:{e.stderr}")
        raise
    post_run_check_balancer(definition, expect_fail)


def post_run_check_balancer(definition: dict[str, Any], expect_fail: bool) -> None:
    for i in range(60):
        time.sleep(1)
        try:
            data = json.loads(
                spawn.capture(
                    [
                        "kubectl",
                        "get",
                        "balancers",
                        "-n",
                        "materialize-environment",
                        "-o",
                        "json",
                    ],
                    stderr=subprocess.DEVNULL,
                )
            )
            status = data["items"][0].get("status")
            if not status:
                continue
            if expect_fail:
                break
            if not status["conditions"] or status["conditions"][0]["type"] != "Ready":
                continue
            if status["conditions"][0]["status"] == "True":
                break
        except subprocess.CalledProcessError:
            pass
    else:
        spawn.runv(
            [
                "kubectl",
                "get",
                "balancers",
                "-n",
                "materialize-environment",
                "-o",
                "yaml",
            ],
        )
        raise ValueError("Never completed")

    for i in range(120):
        print("kubectl get balancer pods")
        try:
            status = spawn.capture(
                [
                    "kubectl",
                    "get",
                    "pods",
                    "-l",
                    "app=balancerd",
                    "-n",
                    "materialize-environment",
                    "-o",
                    "jsonpath={.items[0].status.phase}",
                ],
                stderr=subprocess.DEVNULL,
            )
            if status in ["Running", "Error", "CrashLoopBackOff"]:
                assert not expect_fail
                break
        except subprocess.CalledProcessError:
            if expect_fail:
                try:
                    logs = spawn.capture(
                        [
                            "kubectl",
                            "logs",
                            "-l",
                            "app.kubernetes.io/instance=operator",
                            "-n",
                            "materialize",
                        ],
                        stderr=subprocess.DEVNULL,
                    )
                    if (
                        f"ERROR k8s_controller::controller: Balancer reconciliation error. err=reconciler for object Balancer.v1alpha1.materialize.cloud/{definition['balancer']['metadata']['name']}.materialize-environment failed"
                        in logs
                    ):
                        break
                except subprocess.CalledProcessError:
                    pass

        time.sleep(1)
    else:
        # Helps to debug
        spawn.runv(
            [
                "kubectl",
                "describe",
                "pod",
                "-l",
                "app=balancerd",
                "-n",
                "materialize-environment",
            ]
        )
        raise ValueError("Never completed")


# OIDC end-to-end testing
#
# Spins up Ory Hydra as a real OIDC provider next to orchestratord/environmentd
# in kind and verifies:
#   1. mz_system password fallback login (Materialize CR backend secret).
#   2. pgwire login as an OIDC user with a JWT issued by Hydra via the
#      client_credentials grant.

OIDC_HYDRA_NAMESPACE = "hydra"
OIDC_HYDRA_IMAGE = "oryd/hydra:v2.2.0"
OIDC_HYDRA_PUBLIC_PORT = 4444
OIDC_HYDRA_ADMIN_PORT = 4445
OIDC_HYDRA_ISSUER = f"http://hydra-public.{OIDC_HYDRA_NAMESPACE}.svc.cluster.local:{OIDC_HYDRA_PUBLIC_PORT}"
OIDC_CLIENT_ID = "mz-test-client"
OIDC_CLIENT_SECRET = "mz-test-client-secret"
OIDC_AUDIENCE = "mz-test-audience"
OIDC_SYSTEM_PARAMS_CM = "oidc-system-params"
OIDC_MZ_SYSTEM_PASSWORD = "oidc-test-mz-system-password"


def _hydra_manifests() -> list[dict[str, Any]]:
    # `hydra serve all --dev` relaxes TLS requirements for the public/admin
    # endpoints, which is required since the kind cluster has no TLS.
    # `secrets.system` must be >= 16 bytes; `dsn: memory` keeps everything
    # in-memory so no separate database is needed for the test.
    hydra_config = yaml.dump(
        {
            "dsn": "memory",
            "secrets": {"system": ["mz-test-secret-please-change-me"]},
            "urls": {
                "self": {"issuer": OIDC_HYDRA_ISSUER},
                "login": "http://unused/login",
                "consent": "http://unused/consent",
            },
            "oauth2": {
                "expose_internal_errors": True,
            },
            "strategies": {"access_token": "jwt"},
            "log": {"level": "info", "leak_sensitive_values": True},
            "ttl": {"access_token": "10m"},
        }
    )
    return [
        {
            "apiVersion": "v1",
            "kind": "Namespace",
            "metadata": {"name": OIDC_HYDRA_NAMESPACE},
        },
        {
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {"name": "hydra-config", "namespace": OIDC_HYDRA_NAMESPACE},
            "data": {"hydra.yaml": hydra_config},
        },
        {
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {"name": "hydra", "namespace": OIDC_HYDRA_NAMESPACE},
            "spec": {
                "replicas": 1,
                "selector": {"matchLabels": {"app": "hydra"}},
                "template": {
                    "metadata": {"labels": {"app": "hydra"}},
                    "spec": {
                        "containers": [
                            {
                                "name": "hydra",
                                "image": OIDC_HYDRA_IMAGE,
                                "imagePullPolicy": "IfNotPresent",
                                "args": [
                                    "serve",
                                    "all",
                                    "--dev",
                                    "--config",
                                    "/etc/hydra/hydra.yaml",
                                ],
                                "ports": [
                                    {"containerPort": OIDC_HYDRA_PUBLIC_PORT},
                                    {"containerPort": OIDC_HYDRA_ADMIN_PORT},
                                ],
                                "readinessProbe": {
                                    "httpGet": {
                                        "path": "/health/ready",
                                        "port": OIDC_HYDRA_ADMIN_PORT,
                                    },
                                    "initialDelaySeconds": 2,
                                    "periodSeconds": 2,
                                },
                                "volumeMounts": [
                                    {
                                        "name": "config",
                                        "mountPath": "/etc/hydra",
                                        "readOnly": True,
                                    }
                                ],
                            }
                        ],
                        "volumes": [
                            {
                                "name": "config",
                                "configMap": {"name": "hydra-config"},
                            }
                        ],
                    },
                },
            },
        },
        {
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {
                "name": "hydra-public",
                "namespace": OIDC_HYDRA_NAMESPACE,
            },
            "spec": {
                "selector": {"app": "hydra"},
                "ports": [
                    {
                        "port": OIDC_HYDRA_PUBLIC_PORT,
                        "targetPort": OIDC_HYDRA_PUBLIC_PORT,
                    }
                ],
            },
        },
        {
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {
                "name": "hydra-admin",
                "namespace": OIDC_HYDRA_NAMESPACE,
            },
            "spec": {
                "selector": {"app": "hydra"},
                "ports": [
                    {
                        "port": OIDC_HYDRA_ADMIN_PORT,
                        "targetPort": OIDC_HYDRA_ADMIN_PORT,
                    }
                ],
            },
        },
    ]


def install_hydra() -> None:
    """Deploy Ory Hydra into the kind cluster and create the test client."""
    spawn.runv(
        ["kubectl", "apply", "-f", "-"],
        stdin=yaml.dump_all(_hydra_manifests()).encode(),
    )
    spawn.runv(
        [
            "kubectl",
            "wait",
            "-n",
            OIDC_HYDRA_NAMESPACE,
            "deployment/hydra",
            "--for=condition=Available",
            "--timeout=300s",
        ]
    )
    _create_hydra_client()


def _create_hydra_client() -> None:
    """Idempotently register the OAuth2 client used by the test."""
    with _port_forward(
        "deployment/hydra",
        OIDC_HYDRA_ADMIN_PORT,
        namespace=OIDC_HYDRA_NAMESPACE,
    ) as admin_port:
        # Delete any pre-existing client of the same id (best effort).
        try:
            requests.delete(
                f"http://127.0.0.1:{admin_port}/admin/clients/{OIDC_CLIENT_ID}",
                timeout=10,
            )
        except requests.RequestException:
            pass
        # `audience` on the client constrains which audiences the client may
        # request; the `aud` claim is asserted in the token request below.
        resp = requests.post(
            f"http://127.0.0.1:{admin_port}/admin/clients",
            json={
                "client_id": OIDC_CLIENT_ID,
                "client_secret": OIDC_CLIENT_SECRET,
                "grant_types": ["client_credentials"],
                "token_endpoint_auth_method": "client_secret_post",
                "audience": [OIDC_AUDIENCE],
                "access_token_strategy": "jwt",
            },
            timeout=10,
        )
        assert resp.status_code in (
            200,
            201,
        ), f"hydra create client failed: {resp.status_code} {resp.text}"


@contextmanager
def _port_forward(
    target: str, port: int, namespace: str, local_port: int | None = None
) -> Iterator[int]:
    """Generic kubectl port-forward context manager."""
    local_port = local_port if local_port is not None else port
    process = subprocess.Popen(
        [
            "kubectl",
            "port-forward",
            target,
            f"{local_port}:{port}",
            "-n",
            namespace,
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        time.sleep(2)
        if process.poll() is not None:
            stdout, stderr = process.communicate()
            raise RuntimeError(
                f"Port forward to {target} failed: "
                f"stdout={stdout.decode()}, stderr={stderr.decode()}"
            )
        yield local_port
    finally:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()


def _fetch_hydra_jwt() -> str:
    """Run a client_credentials grant against Hydra and return the access token."""
    with _port_forward(
        "deployment/hydra",
        OIDC_HYDRA_PUBLIC_PORT,
        namespace=OIDC_HYDRA_NAMESPACE,
    ) as public_port:
        resp = requests.post(
            f"http://127.0.0.1:{public_port}/oauth2/token",
            data={
                "grant_type": "client_credentials",
                "client_id": OIDC_CLIENT_ID,
                "client_secret": OIDC_CLIENT_SECRET,
                "audience": OIDC_AUDIENCE,
            },
            timeout=10,
        )
        assert (
            resp.status_code == 200
        ), f"hydra token grant failed: {resp.status_code} {resp.text}"
        body = resp.json()
        token = body.get("access_token")
        assert token, f"no access_token in response: {body}"
        # Cheap sanity check that we got a JWT (three base64 segments), not an
        # opaque reference token; a misconfigured strategy would silently
        # return an opaque token that environmentd can't validate.
        assert token.count(".") == 2, f"hydra returned non-JWT token: {token!r}"
        return token


def _oidc_system_params_configmap() -> dict[str, Any]:
    return {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {
            "name": OIDC_SYSTEM_PARAMS_CM,
            "namespace": "materialize-environment",
        },
        "data": {
            "system-params.json": json.dumps(
                {
                    "oidc_issuer": OIDC_HYDRA_ISSUER,
                    "oidc_audience": [OIDC_AUDIENCE],
                    "oidc_authentication_claim": "sub",
                }
            )
        },
    }


def workflow_oidc_auth(c: Composition, parser: WorkflowArgumentParser) -> None:
    """End-to-end OIDC auth test via orchestratord + Ory Hydra."""
    parser.add_argument(
        "--recreate-cluster",
        action=argparse.BooleanOptionalAction,
        help="Recreate cluster if it exists already",
    )
    parser.add_argument(
        "--tag",
        type=str,
        help="Custom version tag to use",
    )
    parser.add_argument(
        "--orchestratord-override",
        default=True,
        action=argparse.BooleanOptionalAction,
        help="Override orchestratord tag",
    )
    args = parser.parse_args()

    version = get_version(args.tag)
    min_version = MzVersion.parse_mz("v26.16.0-dev.0")
    if version < min_version:
        raise ValueError(
            f"workflow_oidc_auth requires environmentd >= {min_version}, got {version}"
        )

    definition = setup(c, args)

    install_hydra()

    definition["materialize"]["spec"]["authenticatorKind"] = "Oidc"
    definition["materialize"]["spec"][
        "systemParameterConfigmapName"
    ] = OIDC_SYSTEM_PARAMS_CM
    definition["secret"]["stringData"][
        "external_login_password_mz_system"
    ] = OIDC_MZ_SYSTEM_PASSWORD
    definition["system_params_configmap"] = _oidc_system_params_configmap()

    init(definition)
    run(definition, expect_fail=False)

    with port_forward_environmentd() as port:
        print("Verifying mz_system password fallback...")
        with (
            psycopg.connect(
                host="127.0.0.1",
                port=port,
                user="mz_system",
                password=OIDC_MZ_SYSTEM_PASSWORD,
                dbname="materialize",
                connect_timeout=30,
            ) as conn,
            conn.cursor() as cur,
        ):
            cur.execute("SELECT current_user")
            row = cur.fetchone()
            assert row == ("mz_system",), f"unexpected current_user: {row}"

        print(f"Fetching JWT from Hydra for client {OIDC_CLIENT_ID}...")
        token = _fetch_hydra_jwt()

        print(f"Verifying OIDC pgwire login as {OIDC_CLIENT_ID}...")
        with (
            psycopg.connect(
                host="127.0.0.1",
                port=port,
                user=OIDC_CLIENT_ID,
                password=token,
                dbname="materialize",
                connect_timeout=30,
            ) as conn,
            conn.cursor() as cur,
        ):
            cur.execute("SELECT current_user")
            row = cur.fetchone()
            assert row == (OIDC_CLIENT_ID,), f"unexpected current_user: {row}"

        print("Verifying OIDC login rejects an invalid token...")
        bad_token = token[:-4] + "AAAA"
        try:
            psycopg.connect(
                host="127.0.0.1",
                port=port,
                user=OIDC_CLIENT_ID,
                password=bad_token,
                dbname="materialize",
                connect_timeout=30,
            ).close()
        except psycopg.OperationalError:
            pass
        else:
            raise AssertionError(
                "OIDC login with tampered token unexpectedly succeeded"
            )

    print("OIDC end-to-end auth test passed.")


def workflow_clusterd_generation_scheduling(
    c: Composition, parser: WorkflowArgumentParser
) -> None:
    """Regression test for CLO-77.

    Clusterd pod scheduling constraints (anti-affinity, topology spread) must
    only consider pods of the same deploy generation. Otherwise, during a
    generation rollout, the still-running old-generation pod can block the
    new-generation pod from scheduling — e.g. when topology spread + a
    single eligible AZ count the old-gen pod toward `maxSkew`, leaving the
    new-gen pod Pending and the rollout stuck.

    The kind nodes in `cluster.yaml.tmpl` happen to make this easy to
    reproduce: only one of the two worker nodes carries
    `materialize.cloud/swap=true`, so all clusterd pods land in that node's
    AZ. With topology spread enabled and `maxSkew=1`, the new-generation
    pod can only schedule there if the spread selector filters to its own
    generation.
    """
    parser.add_argument(
        "--recreate-cluster",
        action=argparse.BooleanOptionalAction,
        help="Recreate cluster if it exists already",
    )
    parser.add_argument("--tag", type=str, help="Custom version tag to use")
    parser.add_argument(
        "--orchestratord-override",
        default=True,
        action=argparse.BooleanOptionalAction,
        help="Override orchestratord tag",
    )
    args = parser.parse_args()

    definition = setup(c, args)
    init(definition)
    run(definition, expect_fail=False)

    gen_label = "cluster.environmentd.materialize.cloud/generation"
    cluster_id_label = "cluster.environmentd.materialize.cloud/cluster-id"

    # Enable topology spread (off by default on self-hosted via
    # `cluster_enable_topology_spread=false`). The internal SQL listener
    # at 6877 has no authenticator, so a plain mz_system connection works.
    with port_forward_environmentd(6877) as port:
        with (
            psycopg.connect(
                f"host=localhost port={port} user=mz_system sslmode=disable",
                autocommit=True,
            ) as conn,
            conn.cursor() as cur,
        ):
            cur.execute("ALTER SYSTEM SET cluster_enable_topology_spread = true")

    def list_clusterd_statefulsets() -> list[dict[str, Any]]:
        # `kubectl get -l` matches against statefulset metadata labels, but
        # clusterd statefulsets only carry labels on the pod template (not
        # on the statefulset itself), so filter client-side.
        data = json.loads(
            spawn.capture(
                [
                    "kubectl",
                    "get",
                    "statefulset",
                    "-n",
                    "materialize-environment",
                    "-o",
                    "json",
                ]
            )
        )
        return [
            ss
            for ss in data["items"]
            if ss["spec"]["template"]["metadata"]
            .get("labels", {})
            .get("environmentd.materialize.cloud/namespace")
            == "cluster"
        ]

    # The ALTER SYSTEM above only takes effect for replicas created after
    # the change, so cycle clusterd to pick up topology spread by forcing
    # a new generation. `requestRollout` alone is a no-op when nothing
    # else in the spec changed; `forceRollout` set to the same value
    # makes orchestratord treat the resources as changed and bump the
    # generation anyway.
    initial_request = definition["materialize"]["spec"].get("requestRollout")
    new_request = str(uuid.uuid4())
    assert new_request != initial_request

    # Park the rollout at `ReadyToPromote` so both the old- and new-
    # generation clusterd pods are alive simultaneously while we inspect
    # them. Without the CLO-77 fix, the new-gen clusterd would never
    # become ready (pod Pending) and `ReadyToPromote` would never fire.
    definition["materialize"]["spec"]["rolloutStrategy"] = "ManuallyPromote"
    definition["materialize"]["spec"]["requestRollout"] = new_request
    definition["materialize"]["spec"]["forceRollout"] = new_request
    spawn.runv(
        ["kubectl", "apply", "-f", "-"],
        stdin=yaml.dump_all(
            [
                definition["namespace"],
                definition["secret"],
                definition["materialize"],
            ]
        ).encode(),
    )

    for _ in range(900):
        time.sleep(1)
        if is_ready_to_manually_promote():
            break
    else:
        spawn.runv(
            [
                "kubectl",
                "get",
                "statefulsets,pods",
                "-n",
                "materialize-environment",
                "-o",
                "wide",
            ]
        )
        raise RuntimeError(
            "rollout never reached ReadyToPromote — likely a clusterd "
            "statefulset for the new generation has a pod stuck Pending "
            "(check that scheduling constraints filter to the same "
            "generation; see CLO-77)"
        )

    # Both generations should be live now. Group statefulsets by replica
    # so we can verify every replica brought up its new generation while
    # the old one was still running.
    statefulsets = list_clusterd_statefulsets()
    assert statefulsets, "no clusterd statefulsets found"
    by_replica: dict[tuple[str, str], dict[str, dict[str, Any]]] = {}
    for ss in statefulsets:
        labels = ss["spec"]["template"]["metadata"]["labels"]
        cluster_id = labels[cluster_id_label]
        replica_id = labels["cluster.environmentd.materialize.cloud/replica-id"]
        gen = labels[gen_label]
        by_replica.setdefault((cluster_id, replica_id), {})[gen] = ss

    # The current orchestratord generation, derived from the parked
    # Materialize CR — used to compute the expected (old, new) pair per
    # replica.
    parked_mz = json.loads(
        spawn.capture(
            [
                "kubectl",
                "get",
                "materializes",
                "-n",
                "materialize-environment",
                "-o",
                "json",
            ]
        )
    )["items"][0]
    new_gen = int(parked_mz["status"]["activeGeneration"]) + 1
    old_gen = new_gen - 1

    errors: list[str] = []
    for (cluster_id, replica_id), gens in sorted(by_replica.items()):
        replica = f"{cluster_id}/{replica_id}"
        present = sorted(int(g) for g in gens)
        if present != [old_gen, new_gen]:
            errors.append(
                f"{replica}: expected statefulsets for generations "
                f"{[old_gen, new_gen]}, got {present}"
            )
            continue
        for gen_value, ss in gens.items():
            if ss["status"].get("readyReplicas") != 1:
                errors.append(f"{replica} gen {gen_value}: not ready: {ss['status']}")

        # Each new-generation statefulset's anti-affinity and topology
        # spread selectors must pin to its own generation. Without that
        # filter, the new-gen pod's spread/anti-affinity would have
        # counted the old-gen pod and (depending on capacity) blocked
        # scheduling.
        new_ss = gens[str(new_gen)]
        pod_template = new_ss["spec"]["template"]
        actual_gen_label = pod_template["metadata"]["labels"].get(gen_label)
        if actual_gen_label != str(new_gen):
            errors.append(
                f"{replica} gen {new_gen}: pod template missing/incorrect "
                f"{gen_label}: {actual_gen_label!r}"
            )

        affinity = pod_template["spec"]["affinity"]
        anti_affinity_terms = affinity["podAntiAffinity"][
            "requiredDuringSchedulingIgnoredDuringExecution"
        ]
        if not anti_affinity_terms:
            errors.append(f"{replica} gen {new_gen}: no required pod anti-affinity")
            continue
        anti_affinity_exprs = anti_affinity_terms[0]["labelSelector"][
            "matchExpressions"
        ]
        if not any(
            e["key"] == gen_label
            and e["operator"] == "In"
            and e["values"] == [str(new_gen)]
            for e in anti_affinity_exprs
        ):
            errors.append(
                f"{replica} gen {new_gen}: anti-affinity does not filter by "
                f"generation; matchExpressions={anti_affinity_exprs}"
            )

        topology_spread = pod_template["spec"]["topologySpreadConstraints"]
        if not topology_spread:
            errors.append(f"{replica} gen {new_gen}: topology spread not present")
            continue
        spread_exprs = topology_spread[0]["labelSelector"]["matchExpressions"]
        if not any(
            e["key"] == gen_label
            and e["operator"] == "In"
            and e["values"] == [str(new_gen)]
            for e in spread_exprs
        ):
            errors.append(
                f"{replica} gen {new_gen}: topology spread does not filter by "
                f"generation; matchExpressions={spread_exprs}"
            )

    assert not errors, "clusterd generation scheduling check failed:\n" + "\n".join(
        errors
    )

    print(
        f"verified {len(by_replica)} replica(s) have generations "
        f"{old_gen} and {new_gen} live and that new-gen selectors filter "
        f"to generation {new_gen}"
    )


def workflow_mz_debug_scaled_replica(
    c: Composition, parser: WorkflowArgumentParser
) -> None:
    """
    We stand up a managed cluster whose one replica has scale=2 (two clusterd
    pods behind one service), run mz-debug, and assert one CPU profile per pod.
    """
    parser.add_argument(
        "--recreate-cluster",
        action=argparse.BooleanOptionalAction,
        help="Recreate cluster if it exists already",
    )
    parser.add_argument("--tag", type=str, help="Custom version tag to use")
    parser.add_argument(
        "--orchestratord-override",
        default=True,
        action=argparse.BooleanOptionalAction,
        help="Override orchestratord tag",
    )
    args = parser.parse_args()

    SCALE = 2
    CLUSTER_NAME = "scaled_dbg"
    # The name mz-debug matches k8s resources against, taken from the testing
    # Materialize CR (`misc/helm-charts/testing/materialize.yaml`).
    MZ_INSTANCE_NAME = "12345678-1234-1234-1234-123456789012"

    definition = setup(c, args)

    # The only scale>1 size the operator ships (`6400cc`) demands 62 CPUs and
    # ~470 GiB per pod, which will never schedule on kind. Register a tiny
    # scale=2 size for the test instead.
    definition["operator"]["operator"]["clusters"]["sizes"]["mz_debug_scale2"] = {
        "workers": 1,
        "scale": SCALE,
        "cpu_exclusive": False,
        "cpu_limit": 0.1,
        "credits_per_hour": "0.00",
        "disk_limit": "1552MiB",
        "memory_limit": "776MiB",
    }

    init(definition)
    run(definition, expect_fail=False)

    # Create a managed cluster whose single replica has scale=2. The internal
    with port_forward_environmentd(6877) as port:
        with (
            psycopg.connect(
                f"host=localhost port={port} user=mz_system sslmode=disable",
                autocommit=True,
            ) as conn,
            conn.cursor() as cur,
        ):
            # `REPLICATION FACTOR 1` pins the cluster to exactly one replica, so
            # the scale=2 size yields exactly two clusterd pods behind one
            # service.
            cur.execute(
                f"CREATE CLUSTER {CLUSTER_NAME} SIZE 'mz_debug_scale2', REPLICATION FACTOR 1"
            )
            cur.execute(
                "SELECT c.id, r.id "
                "FROM mz_cluster_replicas r "
                "JOIN mz_clusters c ON r.cluster_id = c.id "
                f"WHERE c.name = '{CLUSTER_NAME}'"
            )
            rows = cur.fetchall()
            assert len(rows) == 1, f"expected exactly one replica, got {rows}"
            cluster_id, replica_id = rows[0]

    cluster_id_selector = (
        f"cluster.environmentd.materialize.cloud/cluster-id={cluster_id}"
    )

    def clusterd_pod_names() -> list[str]:
        pods = json.loads(
            spawn.capture(
                [
                    "kubectl",
                    "get",
                    "pods",
                    "-l",
                    cluster_id_selector,
                    "-n",
                    "materialize-environment",
                    "-o",
                    "json",
                ]
            )
        )["items"]
        return sorted(p["metadata"]["name"] for p in pods)

    # The clusterd pods are created asynchronously after the catalog commit, so
    # wait for them to appear before waiting on readiness (`kubectl wait` errors
    # when no pods match its selector).
    for _ in range(300):
        if len(clusterd_pod_names()) >= SCALE:
            break
        time.sleep(1)
    else:
        raise RuntimeError(
            f"scaled replica never brought up {SCALE} clusterd pods: "
            f"{clusterd_pod_names()}"
        )

    spawn.runv(
        [
            "kubectl",
            "wait",
            "--for=condition=Ready",
            "pod",
            "-l",
            cluster_id_selector,
            "-n",
            "materialize-environment",
            "--timeout=300s",
        ]
    )

    pod_names = clusterd_pod_names()
    assert (
        len(pod_names) == SCALE
    ), f"expected {SCALE} clusterd pods for the scaled replica, got {pod_names}"

    # Run mz-debug, capturing only CPU profiles to keep the run fast and the
    # trigger isolated. `--dump-cpu-profiles` is on by default; we disable the
    # other collectors. The connection URL is only parsed (system-catalog dump
    # is off), so no live SQL connection is needed.
    spawn.runv(
        [
            "./mz-debug",
            "self-managed",
            "--k8s-namespace",
            "materialize-environment",
            "--mz-instance-name",
            MZ_INSTANCE_NAME,
            "--mz-connection-url",
            "postgresql://mz_system@localhost:6877/materialize",
            "--dump-k8s=false",
            "--dump-system-catalog=false",
            "--dump-heap-profiles=false",
            "--dump-prometheus-metrics=false",
            "--dump-cpu-profiles=true",
            "--cpu-profile-duration-seconds=1",
        ]
    )

    # mz-debug writes `mz_debug_<timestamp>/profiles/<service>.cpuprof.pprof.gz`
    # to its working directory. Match the scaled replica's files by its unique
    # cluster/replica id, which appears in the clusterd service name regardless
    # of the operator's name prefix or deploy generation.
    replica_marker = f"cluster-{cluster_id}-replica-{replica_id}"
    all_cpu_profiles = sorted(MZ_ROOT.glob("mz_debug_*/profiles/*.cpuprof.pprof.gz"))
    matching = [p for p in all_cpu_profiles if replica_marker in p.name]
    print(
        f"CPU profiles for replica {cluster_id}/{replica_id}: "
        f"{[p.name for p in matching]} (all: {[p.name for p in all_cpu_profiles]})"
    )

    assert matching, (
        "mz-debug produced no CPU profile for the scaled replica's clusterd "
        "service, so cluster discovery or CPU capture failed and Finding 2 "
        f"cannot be assessed. All CPU profiles: {[p.name for p in all_cpu_profiles]}"
    )
    assert len(matching) == len(pod_names), (
        f"mz-debug captured {len(matching)} CPU profile(s) "
        f"({[p.name for p in matching]}) for the scale-{SCALE} replica, but it "
        f"has {len(pod_names)} clusterd pods ({pod_names}). "
        "`kubectl port-forward service/...` profiled only one arbitrary pod; "
        "every pod behind the service must be profiled, with the pod ordinal in "
        "the filename."
    )

    print(
        f"verified mz-debug captured a CPU profile for all {len(pod_names)} "
        f"pods of the scale-{SCALE} replica {cluster_id}/{replica_id}"
    )
