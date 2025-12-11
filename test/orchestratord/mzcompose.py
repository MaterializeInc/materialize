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
    return f'{image.rsplit(":", 1)[0]}:{get_tag(tag)}'


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

    def __eq__(self, other: "Modification"):
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
        retry(check, 120)


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
        retry(check, 120)


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
            result = spawn.capture(
                [
                    "kubectl",
                    "get",
                    "pods",
                    "-l",
                    "app=console",
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
            image = environmentd["items"][0]["spec"]["containers"][0]["image"]
            expected = f"materialize/environmentd:{self.value}"
            assert (
                image == expected or f"ghcr.io/materializeinc/{image}" == expected
            ), f"Expected environmentd image {expected}, but found {image}"

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

        orchestratord = get_orchestratord_data()
        args = orchestratord["items"][0]["spec"]["containers"][0]["args"]
        expected = "--collect-pod-metrics"
        if self.value and mods[ObservabilityEnabled]:
            assert (
                expected in args
            ), f"Expected {expected} in environmentd args, but only found {args}"
        else:
            assert (
                expected not in args
            ), f"Expected no {expected} in environmentd args, but found it: {args}"


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
            num_pods = len(console["items"])
            expected = self.value if self.value is not None else 2
            assert (
                num_pods == expected
            ), f"Expected {expected} console pods, but found {num_pods}"

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

            resources = clusterd["spec"]["containers"][0]["resources"]
            validate_container_resources(resources, mods[SwapEnabledGlobal])

            node_selector = clusterd["spec"]["nodeSelector"]
            # checking this one separately, since it isn't in the cluster size's selector
            assert node_selector["workload"] == "materialize-instance"
            validate_node_selector(node_selector, mods[SwapEnabledGlobal], self.value)

        # Clusterd can take a while to start up
        retry(check_pods, 5)


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
            console = get_console_data()["items"][0]

            resources = console["spec"]["containers"][0]["resources"]
            assert (
                resources == expected
            ), f"Expected console resources {expected}, but got {resources}"

        retry(check_pods, 240)


class AuthenticatorKind(Modification):
    @classmethod
    def values(cls, version: MzVersion) -> list[Any]:
        # Test None, Password (v0.147.7+), and Sasl
        result = ["None"]
        if version >= MzVersion.parse_mz("v0.147.7"):
            result.append("Password")
        if version >= MzVersion.parse_mz("v26.0.0"):
            result.append("Sasl")
        return result

    @classmethod
    def default(cls) -> Any:
        return "None"

    def modify(self, definition: dict[str, Any]) -> None:
        definition["materialize"]["spec"]["authenticatorKind"] = self.value
        if self.value == "Password" or self.value == "Sasl":
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

        port = (
            6875
            if (version >= MzVersion.parse_mz("v0.147.0") and self.value == "Password")
            or (version >= MzVersion.parse_mz("v26.0.0") and self.value == "Sasl")
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
    versions = reversed(
        [
            version
            for version in get_self_managed_versions()
            if version < current_version
        ]
        + [current_version]
    )
    for version in versions:
        print(f"--- Running with defaults against {version}")
        dir = "my-local-mz"
        if os.path.exists(dir):
            shutil.rmtree(dir)
        os.mkdir(dir)
        spawn.runv(["kind", "delete", "cluster"])
        spawn.runv(["kind", "create", "cluster"])
        spawn.runv(
            [
                "kubectl",
                "label",
                "node",
                "kind-control-plane",
                "materialize.cloud/disk=true",
            ]
        )
        spawn.runv(
            [
                "kubectl",
                "label",
                "node",
                "kind-control-plane",
                "materialize.cloud/swap=true",
            ]
        )
        spawn.runv(
            [
                "kubectl",
                "label",
                "node",
                "kind-control-plane",
                "workload=materialize-instance",
            ]
        )

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
                url = f"https://raw.githubusercontent.com/MaterializeInc/materialize/refs/tags/{version}/{path}"
                response = requests.get(url)
                assert (
                    response.status_code == 200
                ), f"Failed to download {file} from {url}: {response.status_code}"
                with open(os.path.join(dir, file), "wb") as f:
                    f.write(response.content)

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
        spawn.runv(
            [
                "helm",
                "install",
                "my-materialize-operator",
                MZ_ROOT / "misc" / "helm-charts" / "operator",
                "--namespace=materialize",
                "--create-namespace",
                "--version",
                "v26.0.0",
                "--set",
                "observability.podMetrics.enabled=true",
                "-f",
                os.path.join(dir, "sample-values.yaml"),
            ]
        )
        spawn.runv(
            ["kubectl", "apply", "-f", os.path.join(dir, "sample-postgres.yaml")]
        )
        spawn.runv(["kubectl", "apply", "-f", os.path.join(dir, "sample-minio.yaml")])
        spawn.runv(["kubectl", "get", "all", "-n", "materialize"])
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
        for i in range(120):
            try:
                spawn.capture(
                    [
                        "kubectl",
                        "get",
                        "crd",
                        "materializes.materialize.cloud",
                        "-n",
                        "materialize",
                        "-o",
                        "name",
                    ],
                    stderr=subprocess.DEVNULL,
                )
                break

            except subprocess.CalledProcessError:
                pass
            time.sleep(1)
        else:
            raise ValueError("Never completed")

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
    definition["materialize"]["spec"]["requestRollout"] = request
    definition["materialize"]["spec"]["forceRollout"] = request
    run(definition, False)
    time.sleep(120)  # some time to make sure there is no downtime later
    running = False
    thread.join()

    assert len(downtimes) == 2, f"Wrong number of downtimes: {downtimes}"

    test_failed = False
    max_downtime = 15
    for downtime in downtimes:
        if downtime > max_downtime:
            print(f"SELECT 1 took more than {max_downtime}s: {downtime}s")
            test_failed = True

    upload_upgrade_downtime_to_test_analytics(
        c, downtimes[0], downtimes[1], not test_failed
    )
    assert not test_failed


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
                current_version = rng.choice(versions)
                chain = [current_version]
                next_version = current_version

                try:
                    for _ in range(len(versions)):
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

    cluster = "kind"
    clusters = spawn.capture(["kind", "get", "clusters"]).strip().split("\n")
    if cluster not in clusters or args.recreate_cluster:
        kind_version = Version.parse(
            spawn.capture(["kind", "version"]).split(" ")[1][1:]
        )
        assert kind_version >= Version.parse(
            "0.29.0"
        ), f"kind >= v0.29.0 required, while you are on {kind_version}"

        spawn.runv(["kind", "delete", "cluster", "--name", cluster])

        try:
            spawn.runv(["docker", "network", "create", "kind"])
        except:
            pass
        try:
            spawn.runv(["docker", "stop", "proxy-dockerhub"])
        except:
            pass
        try:
            spawn.runv(["docker", "rm", "proxy-dockerhub"])
        except:
            pass
        try:
            spawn.runv(["docker", "stop", "proxy-ghcr"])
        except:
            pass
        try:
            spawn.runv(["docker", "rm", "proxy-ghcr"])
        except:
            pass

        dockerhub_username = os.getenv("DOCKERHUB_USERNAME")
        dockerhub_token = os.getenv("DOCKERHUB_ACCESS_TOKEN")
        spawn.runv(
            [
                "docker",
                "run",
                "-d",
                "--name",
                "proxy-dockerhub",
                "--restart=always",
                "--net=kind",
                "-v",
                f"{MZ_ROOT}/misc/kind/cache/dockerhub:/var/lib/registry",
                "-e",
                "REGISTRY_PROXY_REMOTEURL=https://registry-1.docker.io",
                *(
                    [
                        "-e",
                        f"REGISTRY_PROXY_USERNAME={dockerhub_username}",
                        "-e",
                        f"REGISTRY_PROXY_PASSWORD={dockerhub_token}",
                    ]
                    if dockerhub_username and dockerhub_token
                    else []
                ),
                "registry:2",
            ]
        )

        ghcr_username = "materialize-bot"
        ghcr_token = os.getenv("GITHUB_GHCR_TOKEN")
        spawn.runv(
            [
                "docker",
                "run",
                "-d",
                "--name",
                "proxy-ghcr",
                "--restart=always",
                "--net=kind",
                "-v",
                f"{MZ_ROOT}/misc/kind/cache/ghcr:/var/lib/registry",
                "-e",
                "REGISTRY_PROXY_REMOTEURL=https://ghcr.io",
                *(
                    [
                        "-e",
                        f"REGISTRY_PROXY_USERNAME={ghcr_username}",
                        "-e",
                        f"REGISTRY_PROXY_PASSWORD={ghcr_token}",
                    ]
                    if ghcr_username and ghcr_token
                    else []
                ),
                "registry:2",
            ]
        )

        with (
            open(MZ_ROOT / "test" / "orchestratord" / "cluster.yaml.tmpl") as in_file,
            open(MZ_ROOT / "test" / "orchestratord" / "cluster.yaml", "w") as out_file,
        ):
            text = in_file.read()
            out_file.write(
                text.replace(
                    "$DOCKER_CONFIG",
                    os.getenv("DOCKER_CONFIG", f'{os.environ["HOME"]}/.docker'),
                )
            )

        spawn.runv(
            [
                "kind",
                "create",
                "cluster",
                "--name",
                cluster,
                "--config",
                MZ_ROOT / "test" / "orchestratord" / "cluster.yaml",
            ]
        )
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
        c.up(*[Service(service, idle=True) for service in services])
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
            upgrade_operator_helm_chart(definition, expect_fail)
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


def init(definition: dict[str, Any]) -> None:
    try:
        spawn.capture(
            ["kubectl", "delete", "namespace", "materialize-environment"],
            stderr=subprocess.DEVNULL,
        )
    except subprocess.CalledProcessError:
        pass
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
            MZ_ROOT / "misc" / "helm-charts" / "operator",
            "--namespace=materialize",
            "--create-namespace",
            "--version",
            "v26.0.0",
            "-f",
            "-",
        ],
        stdin=yaml.dump(definition["operator"]).encode(),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    for i in range(240):
        try:
            spawn.capture(
                [
                    "kubectl",
                    "get",
                    "crd",
                    "materializes.materialize.cloud",
                    "-n",
                    "materialize",
                    "-o",
                    "name",
                ],
                stderr=subprocess.DEVNULL,
            )
            break

        except subprocess.CalledProcessError:
            pass
        time.sleep(1)
    else:
        raise ValueError("Never completed")


def upgrade_operator_helm_chart(definition: dict[str, Any], expect_fail: bool) -> None:
    spawn.runv(
        [
            "helm",
            "upgrade",
            "operator",
            MZ_ROOT / "misc" / "helm-charts" / "operator",
            "--namespace=materialize",
            "--version",
            "v26.0.0",
            "-f",
            "-",
        ],
        stdin=yaml.dump(definition["operator"]).encode(),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def run(definition: dict[str, Any], expect_fail: bool) -> None:
    defs = [
        definition["namespace"],
        definition["secret"],
        definition["materialize"],
    ]
    if "materialize2" in definition:
        defs.append(definition["materialize2"])
    if "system_params_configmap" in definition:
        defs.append(definition["system_params_configmap"])
    try:
        spawn.runv(
            ["kubectl", "apply", "-f", "-"],
            stdin=yaml.dump_all(defs).encode(),
        )
    except subprocess.CalledProcessError as e:
        print(f"Failed to apply: {e.stdout}\nSTDERR:{e.stderr}")
        raise

    if definition["materialize"]["spec"].get("rolloutStrategy") == "ManuallyPromote":
        # First wait for it to become ready to promote, but not yet promoted
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
            raise RuntimeError("Never became ready for manual promotion")

        # Wait to see that it doesn't promote
        time.sleep(30)
        if not is_ready_to_manually_promote():
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
            raise RuntimeError(
                "Stopped being ready for manual promotion before promoting"
            )

        # Manually promote it
        mz = json.loads(
            spawn.capture(
                [
                    "kubectl",
                    "get",
                    "materializes",
                    "-n",
                    "materialize-environment",
                    "-o",
                    "json",
                ],
                stderr=subprocess.DEVNULL,
            )
        )["items"][0]
        definition["materialize"]["spec"]["forcePromote"] = mz["spec"]["requestRollout"]
        try:
            spawn.runv(
                ["kubectl", "apply", "-f", "-"],
                stdin=yaml.dump(definition["materialize"]).encode(),
            )
        except subprocess.CalledProcessError as e:
            print(f"Failed to apply: {e.stdout}\nSTDERR:{e.stderr}")
            raise

    post_run_check(definition, expect_fail)


def is_ready_to_manually_promote():
    data = json.loads(
        spawn.capture(
            [
                "kubectl",
                "get",
                "materializes",
                "-n",
                "materialize-environment",
                "-o",
                "json",
            ],
            stderr=subprocess.DEVNULL,
        )
    )
    conditions = data["items"][0].get("status", {}).get("conditions")
    return (
        conditions is not None
        and conditions[0]["type"] == "UpToDate"
        and conditions[0]["status"] == "Unknown"
        and conditions[0]["reason"] == "ReadyToPromote"
    )


def post_run_check(definition: dict[str, Any], expect_fail: bool) -> None:
    for i in range(900):
        time.sleep(1)
        try:
            data = json.loads(
                spawn.capture(
                    [
                        "kubectl",
                        "get",
                        "materializes",
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
            if (
                not status["conditions"]
                or status["conditions"][0]["type"] != "UpToDate"
                or status["conditions"][0]["status"] != "True"
            ):
                continue
            if (
                status["lastCompletedRolloutRequest"]
                == data["items"][0]["spec"]["requestRollout"]
            ):
                break
        except subprocess.CalledProcessError:
            pass
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
                        f"ERROR k8s_controller::controller: Materialize reconciliation error. err=reconciler for object Materialize.v1alpha1.materialize.cloud/{definition['materialize']['metadata']['name']}.materialize-environment failed"
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
