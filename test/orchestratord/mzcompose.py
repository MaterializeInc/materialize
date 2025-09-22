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
import signal
import subprocess
import time
import uuid
from collections.abc import Callable
from enum import Enum
from typing import Any

import psycopg
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
from materialize.version_list import get_all_self_managed_versions

SERVICES = [
    Testdrive(),
    Orchestratord(),
    Environmentd(),
    Clusterd(),
    Balancerd(),
]


def get_image(image: str, tag: str | None) -> str:
    # We can't use the mzbuild tag because it has a different fingerprint for
    # environmentd/clusterd/balancerd and the orchestratord depends on them
    # being identical.
    tag = tag or f"v{ci_util.get_mz_version()}--pr.g{git.rev_parse('HEAD')}"

    return f'{image.rsplit(":", 1)[0]}:{tag}'


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
        # TODO: Add back "del" when https://github.com/MaterializeInc/database-issues/issues/9599 is resolved
        return ["valid", "invalid"]

    @classmethod
    def failed_reconciliation_values(cls) -> list[Any]:
        return ["invalid"]

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

        environmentd = get_environmentd_data()
        if self.value == "invalid":
            assert len(environmentd["items"]) == 0
            return

        envs = environmentd["items"][0]["spec"]["containers"][0]["env"]
        if self.value == "del" or (mods[LicenseKeyCheck] == False):
            for env in envs:
                assert (
                    env["name"] != "MZ_LICENSE_KEY"
                ), f"Expected MZ_LICENSE_KEY to be missing, but is in {envs}"
        elif self.value == "valid":
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
            "v0.147.0"
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
        if MzVersion.parse_mz(mods[EnvironmentdImageRef]) < MzVersion.parse_mz(
            "v0.147.0"
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
            get_image(None).rsplit(":", 1)[1]
        ]

    @classmethod
    def default(cls) -> Any:
        return get_image(None).rsplit(":", 1)[1]

    def modify(self, definition: dict[str, Any]) -> None:
        definition["materialize"]["spec"][
            "environmentdImageRef"
        ] = f"materialize/environmentd:{self.value}"

    def validate(self, mods: dict[type[Modification], Any]) -> None:
        environmentd = get_environmentd_data()
        image = environmentd["items"][0]["spec"]["containers"][0]["image"]
        expected = f"materialize/environmentd:{self.value}"
        assert (
            image == expected
        ), f"Expected environmentd image {expected}, but found {image}"


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


class BalancerdReplicas(Modification):
    @classmethod
    def values(cls) -> list[Any]:
        return [None, 1, 2]

    @classmethod
    def default(cls) -> Any:
        return None

    def modify(self, definition: dict[str, Any]) -> None:
        if self.value is not None:
            definition["materialize"]["spec"]["balancerdReplicas"] = self.value

    def validate(self, mods: dict[type[Modification], Any]) -> None:
        if not mods[BalancerdEnabled]:
            return

        def check_replicas():
            balancerd = get_balancerd_data()
            num_pods = len(balancerd["items"])
            expected = self.value if self.value is not None else 2
            assert (
                num_pods == expected
            ), f"Expected {expected} balancerd pods, but found {num_pods}"

        retry(check_replicas, 120)


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
        retry(check_pods, 5)

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
        return ["None", "Password"]

    @classmethod
    def default(cls) -> Any:
        return "None"

    def modify(self, definition: dict[str, Any]) -> None:
        definition["materialize"]["spec"]["authenticatorKind"] = self.value
        if self.value == "Password":
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

        port = (
            6875
            if version >= MzVersion.parse_mz("v0.147.0") and self.value == "Password"
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
            # ret = process.poll()
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
            psycopg.connect(
                host="127.0.0.1",
                user="mz_system",
                password="superpassword" if self.value == "Password" else None,
                dbname="materialize",
                port=port,
            )
        finally:
            os.killpg(os.getpgid(process.pid), signal.SIGTERM)


class Scenario(Enum):
    Individual = "individual"
    Combine = "combine"
    Defaults = "defaults"
    Upgrade = "upgrade"
    UpgradeChain = "upgrade-chain"

    @classmethod
    def _missing_(cls, value):
        if value == "random":
            return cls(random.choice([elem.value for elem in cls]))


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
    parser.add_argument("--scenario", type=str, default="all")
    parser.add_argument("--modification", action="append", type=str, default=[])
    parser.add_argument("--runtime", type=int, help="Runtime in seconds")
    args = parser.parse_args()

    print(f"--- Random seed is {args.seed}")

    kind_version = Version.parse(spawn.capture(["kind", "version"]).split(" ")[1][1:])
    assert kind_version >= Version.parse(
        "0.29.0"
    ), f"kind >= v0.29.0 required, while you are on {kind_version}"

    # Start up services and potentially compile them first so that we have all images locally
    c.up(
        Service("testdrive", idle=True),
        Service("orchestratord", idle=True),
        Service("environmentd", idle=True),
        Service("clusterd", idle=True),
        Service("balancerd", idle=True),
    )

    cluster = "kind"
    clusters = spawn.capture(["kind", "get", "clusters"]).strip().split("\n")
    if cluster not in clusters or args.recreate_cluster:
        setup(cluster)

    if not args.tag:
        # Start up services and potentially compile them first so that we have all images locally
        c.down(destroy_volumes=True)
        c.up(
            Service("testdrive", idle=True),
            Service("orchestratord", idle=True),
            Service("environmentd", idle=True),
            Service("clusterd", idle=True),
            Service("balancerd", idle=True),
        )
        services = [
            "orchestratord",
            "environmentd",
            "clusterd",
            "balancerd",
        ]
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

    definition["operator"]["operator"]["image"]["tag"] = get_image(
        c.compose["services"]["orchestratord"]["image"], args.tag
    ).rsplit(":", 1)[1]
    # Necessary for upgrades
    definition["operator"]["networkPolicies"]["enabled"] = True
    definition["operator"]["networkPolicies"]["internal"]["enabled"] = True
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

    rng = random.Random(args.seed)

    mod_classes = sorted(all_modifications(), key=repr)
    if args.modification:
        mod_classes = [
            mod_class
            for mod_class in mod_classes
            if mod_class.__name__ in args.modification
        ]

    if not args.scenario[0].isalpha():
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

    scenario = Scenario(args.scenario)
    if scenario == Scenario.Individual:
        assert not args.runtime
        for mod_class in mod_classes:
            for value in mod_class.values():
                run_scenario([[mod_class(value)]], definition)
    elif scenario == Scenario.Combine:
        assert args.runtime
        end_time = (
            datetime.datetime.now() + datetime.timedelta(seconds=args.runtime)
        ).timestamp()
        while time.time() < end_time:
            run_scenario(
                [
                    [
                        mod_class(rng.choice(mod_class.good_values()))
                        for mod_class in mod_classes
                    ]
                ],
                definition,
            )
    elif scenario == Scenario.Defaults:
        assert not args.runtime
        mods = [mod_class(mod_class.default()) for mod_class in mod_classes]
        run_scenario([mods], definition, modify=False)
    elif scenario == Scenario.Upgrade:
        assert args.runtime
        end_time = (
            datetime.datetime.now() + datetime.timedelta(seconds=args.runtime)
        ).timestamp()
        versions = get_all_self_managed_versions()
        while time.time() < end_time:
            versions = sorted(list(rng.sample(versions, 2)))
            run_scenario(
                [
                    [EnvironmentdImageRef(str(version))]
                    + [
                        mod_class(rng.choice(mod_class.good_values()))
                        for mod_class in mod_classes
                    ]
                    for version in versions
                ],
                definition,
            )
    elif scenario == Scenario.UpgradeChain:
        assert args.runtime
        end_time = (
            datetime.datetime.now() + datetime.timedelta(seconds=args.runtime)
        ).timestamp()
        versions = get_all_self_managed_versions()
        while time.time() < end_time:
            n = random.randint(2, len(versions))
            versions = sorted(list(rng.sample(versions, n)))
            run_scenario(
                [
                    [EnvironmentdImageRef(str(version))]
                    + [
                        mod_class(rng.choice(mod_class.good_values()))
                        for mod_class in mod_classes
                    ]
                    for version in versions
                ],
                definition,
            )
    else:
        raise ValueError(f"Unhandled scenario {scenario}")


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
            # TODO: rolling upgrades too
            definition["materialize"]["spec"]["inPlaceRollout"] = True
            definition["materialize"]["spec"]["requestRollout"] = str(uuid.uuid4())
            run(definition, expect_fail)
        if initialize:
            init(definition)
            run(definition, expect_fail)
            initialize = False  # only initialize once
        else:
            upgrade(definition)
        mod_dict = {mod.__class__: mod.value for mod in mods}
        for subclass in all_modifications():
            if subclass not in mod_dict:
                mod_dict[subclass] = subclass.default()
        try:
            for mod in mods:
                mod.validate(mod_dict)
        except:
            print(
                f"Reproduce with bin/mzcompose --find orchestratord run default --scenario='{scenario_json}'"
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


def upgrade(definition: dict[str, Any]) -> None:
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


def run(definition: dict[str, Any], expect_fail: bool) -> None:
    apply_input = yaml.dump_all(
        [
            definition["namespace"],
            definition["secret"],
            definition["materialize"],
        ]
    )
    try:
        spawn.runv(
            ["kubectl", "apply", "-f", "-"],
            stdin=apply_input.encode(),
        )
    except subprocess.CalledProcessError as e:
        print(f"Failed to apply: {e.stdout}\nSTDERR:{e.stderr}")
        raise

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
    time.sleep(10)
