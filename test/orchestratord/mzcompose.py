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
from enum import Enum
from typing import Any

import requests
import yaml
from semver.version import Version

from materialize import MZ_ROOT, ci_util, git, spawn
from materialize.mz_version import MzVersion
from materialize.mzcompose.composition import (
    Composition,
    Service,
    WorkflowArgumentParser,
)
from materialize.mzcompose.services.balancerd import Balancerd
from materialize.mzcompose.services.clusterd import Clusterd
from materialize.mzcompose.services.environmentd import Environmentd
from materialize.mzcompose.services.orchestratord import Orchestratord
from materialize.mzcompose.services.testdrive import Testdrive
from materialize.util import all_subclasses
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
]


def get_tag(tag: str | None = None) -> str:
    # We can't use the mzbuild tag because it has a different fingerprint for
    # environmentd/clusterd/balancerd and the orchestratord depends on them
    # being identical.
    return tag or f"v{ci_util.get_mz_version()}--pr.g{git.rev_parse('HEAD')}"


def get_image(image: str, tag: str | None) -> str:
    return f'{image.rsplit(":", 1)[0]}:{get_tag(tag)}'


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
# TODO: Cover https://materialize.com/docs/self-managed/v25.2/installation/configuration/


class Modification:
    pick_by_default: bool = True

    def __init__(self, value: Any):
        assert value in self.values(), f"Expected {value} to be in {self.values()}"
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
    def values(cls) -> list[Any]:
        raise NotImplementedError

    @classmethod
    def bad_values(cls) -> list[Any]:
        return cls.failed_reconciliation_values()

    @classmethod
    def good_values(cls) -> list[Any]:
        return [value for value in cls.values() if value not in cls.bad_values()]

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
    def values(cls) -> list[Any]:
        return ["valid", "invalid", "del"]

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
            raise ValueError(f"Unknown value {self.value}, only know {self.values()}")

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
    def values(cls) -> list[Any]:
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
    def values(cls) -> list[Any]:
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
    def values(cls) -> list[Any]:
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
    def values(cls) -> list[Any]:
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
    def values(cls) -> list[Any]:
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
    def values(cls) -> list[Any]:
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
                image == expected
            ), f"Expected environmentd image {expected}, but found {image}"

        retry(check, 240)


class NumMaterializeEnvironments(Modification):
    # Only done intentionally
    pick_by_default = False

    @classmethod
    def values(cls) -> list[Any]:
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
    def values(cls) -> list[Any]:
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
    def values(cls) -> list[Any]:
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
    def values(cls) -> list[Any]:
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
    def values(cls) -> list[Any]:
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
    def values(cls) -> list[Any]:
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
#     def values(cls) -> list[Any]:
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
    def values(cls) -> list[Any]:
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
        assert "swap_enabled" not in size
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
    def values(cls) -> list[Any]:
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
    def values(cls) -> list[Any]:
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
    def values(cls) -> list[Any]:
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
    def values(cls) -> list[Any]:
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
    def values(cls) -> list[Any]:
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
    def values(cls) -> list[Any]:
        # Test None, Password (v0.147.7+), and Sasl (v0.147.16+)
        return ["None", "Password", "Sasl"]

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

        if self.value == "Sasl" and version < MzVersion.parse_mz("v0.147.16"):
            return

        port = (
            6875
            if (version >= MzVersion.parse_mz("v0.147.0") and self.value == "Password")
            or (version >= MzVersion.parse_mz("v0.147.16") and self.value == "Sasl")
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


class Properties(Enum):
    Defaults = "defaults"
    Individual = "individual"
    Combine = "combine"


class Action(Enum):
    Noop = "noop"
    Upgrade = "upgrade"
    UpgradeChain = "upgrade-chain"


def workflow_defaults(c: Composition, parser: WorkflowArgumentParser) -> None:
    parser.add_argument(
        "--tag",
        type=str,
        help="Custom version tag to use",
    )
    args = parser.parse_args()

    current_version = get_tag(args.tag)

    # Following https://materialize.com/docs/self-managed/v25.2/installation/install-on-local-kind/
    for version in reversed(get_self_managed_versions() + [current_version]):
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
                "v25.3.0",
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

    kind_version = Version.parse(spawn.capture(["kind", "version"]).split(" ")[1][1:])
    assert kind_version >= Version.parse(
        "0.29.0"
    ), f"kind >= v0.29.0 required, while you are on {kind_version}"

    c.up(Service("testdrive", idle=True))

    cluster = "kind"
    clusters = spawn.capture(["kind", "get", "clusters"]).strip().split("\n")
    if cluster not in clusters or args.recreate_cluster:
        setup(cluster)

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

    def get_mods() -> Iterator[list[Modification]]:
        if properties == Properties.Defaults:
            yield [mod_class(mod_class.default()) for mod_class in mod_classes]
            yield [NumMaterializeEnvironments(2)]
        elif properties == Properties.Individual:
            for mod_class in mod_classes:
                for value in mod_class.values():
                    yield [mod_class(value)]
        elif properties == Properties.Combine:
            assert args.runtime
            while time.time() < end_time:
                yield [
                    mod_class(rng.choice(mod_class.good_values()))
                    for mod_class in mod_classes
                ]
        else:
            raise ValueError(f"Unhandled properties value {properties}")

    mods_it = get_mods()

    try:
        if action == Action.Noop:
            for mods in mods_it:
                run_scenario([mods], definition)
        elif action == Action.Upgrade:
            assert args.runtime
            end_time = (
                datetime.datetime.now() + datetime.timedelta(seconds=args.runtime)
            ).timestamp()
            versions = get_all_self_managed_versions()
            while time.time() < end_time:
                selected_versions = sorted(list(rng.sample(versions, 2)))
                try:
                    mod = next(mods_it)
                except StopIteration:
                    mods_it = get_mods()
                    mod = next(mods_it)
                scenario = [
                    [EnvironmentdImageRef(str(version))] + mod
                    for version in selected_versions
                ]
                run_scenario(scenario, definition)
        elif action == Action.UpgradeChain:
            assert args.runtime
            end_time = (
                datetime.datetime.now() + datetime.timedelta(seconds=args.runtime)
            ).timestamp()
            versions = get_all_self_managed_versions()
            while time.time() < end_time:
                random.randint(2, len(versions))
                selected_versions = sorted(list(rng.sample(versions, 2)))
                try:
                    mod = next(mods_it)
                except StopIteration:
                    mods_it = get_mods()
                    mod = next(mods_it)
                scenario = [
                    [EnvironmentdImageRef(str(version))] + mod for version in versions
                ]
                assert len(scenario) == len(
                    versions
                ), f"Expected scenario with {len(versions)} steps, but only found: {scenario}"
                run_scenario(scenario, definition)
        else:
            raise ValueError(f"Unhandled action {action}")
    except StopIteration:
        pass


def setup(cluster: str):
    spawn.runv(["kind", "delete", "cluster", "--name", cluster])
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
        if not initialize:
            definition["materialize"]["spec"][
                "rolloutStrategy"
            ] = "ImmediatelyPromoteCausingDowntime"
            definition["materialize"]["spec"]["requestRollout"] = str(uuid.uuid4())
            run(definition, expect_fail)
        if initialize:
            init(definition)
            run(definition, expect_fail)
            initialize = False  # only initialize once
        else:
            upgrade(definition, expect_fail)
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
            "v25.3.0",
            "-f",
            "-",
        ],
        stdin=yaml.dump(definition["operator"]).encode(),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
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


def upgrade(definition: dict[str, Any], expect_fail: bool) -> None:
    spawn.runv(
        [
            "helm",
            "upgrade",
            "operator",
            MZ_ROOT / "misc" / "helm-charts" / "operator",
            "--namespace=materialize",
            "--version",
            "v25.3.0",
            "-f",
            "-",
        ],
        stdin=yaml.dump(definition["operator"]).encode(),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    post_run_check(definition, expect_fail)


def run(definition: dict[str, Any], expect_fail: bool) -> None:
    defs = [
        definition["namespace"],
        definition["secret"],
        definition["materialize"],
    ]
    if "materialize2" in definition:
        defs.append(definition["materialize2"])
    try:
        spawn.runv(
            ["kubectl", "apply", "-f", "-"],
            stdin=yaml.dump_all(defs).encode(),
        )
    except subprocess.CalledProcessError as e:
        print(f"Failed to apply: {e.stdout}\nSTDERR:{e.stderr}")
        raise
    post_run_check(definition, expect_fail)


def post_run_check(definition: dict[str, Any], expect_fail: bool) -> None:
    for i in range(60):
        try:
            spawn.capture(
                [
                    "kubectl",
                    "get",
                    "materializes",
                    "-n",
                    "materialize-environment",
                ],
                stderr=subprocess.DEVNULL,
            )
            break
        except subprocess.CalledProcessError:
            pass
        time.sleep(1)
    else:
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
    # Wait a bit for the status to stabilize
    time.sleep(60)
