# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


from materialize.checks.actions import (
    Action,
    BumpVersion,
    GitResetHard,
    Initialize,
    Manipulate,
    Validate,
)
from materialize.checks.checks import Check
from materialize.checks.common import wait_ready_and_promote
from materialize.checks.executors import Executor
from materialize.checks.features import Features
from materialize.checks.mzcompose_actions import (
    StartMz,
)
from materialize.checks.scenarios import Scenario
from materialize.checks.scenarios_upgrade import (
    get_last_version,
    get_minor_versions,
    get_previous_version,
    start_mz_read_only,
)
from materialize.mz_version import MzVersion
from materialize.mzcompose import get_default_system_parameters
from materialize.version_list import VersionsFromDocs


class ZeroDowntimeRestartEntireMz(Scenario):
    def actions(self) -> list[Action]:
        system_parameter_defaults = get_default_system_parameters()
        return [
            StartMz(
                self,
                mz_service="mz_1",
                system_parameter_defaults=system_parameter_defaults,
            ),
            Initialize(self, mz_service="mz_1"),
            start_mz_read_only(
                self,
                deploy_generation=1,
                mz_service="mz_2",
                system_parameter_defaults=system_parameter_defaults,
            ),
            Manipulate(self, phase=1, mz_service="mz_1"),
            *wait_ready_and_promote("mz_2"),
            start_mz_read_only(
                self,
                deploy_generation=2,
                mz_service="mz_3",
                system_parameter_defaults=system_parameter_defaults,
            ),
            Manipulate(self, phase=2, mz_service="mz_2"),
            *wait_ready_and_promote("mz_3"),
            start_mz_read_only(
                self,
                deploy_generation=3,
                mz_service="mz_4",
                system_parameter_defaults=system_parameter_defaults,
            ),
            Validate(self, mz_service="mz_3"),
            *wait_ready_and_promote("mz_4"),
            Validate(self, mz_service="mz_4"),
        ]


class ZeroDowntimeRestartEntireMzForcedMigrations(Scenario):
    def does_forced_migrations(self) -> bool:
        return True

    def actions(self) -> list[Action]:
        system_parameter_defaults = get_default_system_parameters()
        return [
            StartMz(
                self,
                mz_service="mz_1",
                system_parameter_defaults=system_parameter_defaults,
            ),
            Initialize(self, mz_service="mz_1"),
            start_mz_read_only(
                self,
                deploy_generation=1,
                mz_service="mz_2",
                system_parameter_defaults=system_parameter_defaults,
                force_migrations="all",
            ),
            Manipulate(self, phase=1, mz_service="mz_1"),
            *wait_ready_and_promote("mz_2"),
            start_mz_read_only(
                self,
                deploy_generation=2,
                mz_service="mz_3",
                system_parameter_defaults=system_parameter_defaults,
                force_migrations="all",
            ),
            Manipulate(self, phase=2, mz_service="mz_2"),
            *wait_ready_and_promote("mz_3"),
            start_mz_read_only(
                self,
                deploy_generation=3,
                mz_service="mz_4",
                system_parameter_defaults=system_parameter_defaults,
                force_migrations="all",
            ),
            Validate(self, mz_service="mz_3"),
            *wait_ready_and_promote("mz_4"),
            Validate(self, mz_service="mz_4"),
        ]


class ZeroDowntimeUpgradeEntireMz(Scenario):
    """0dt upgrade of the entire Mz instance from the last released version."""

    def base_version(self) -> MzVersion:
        return get_last_version()

    def actions(self) -> list[Action]:
        print(f"Upgrading from tag {self.base_version()}")
        system_parameter_defaults = get_default_system_parameters(self.base_version())
        return [
            StartMz(
                self,
                tag=self.base_version(),
                mz_service="mz_1",
                system_parameter_defaults=system_parameter_defaults,
            ),
            Initialize(self, mz_service="mz_1"),
            start_mz_read_only(
                self,
                tag=None,
                deploy_generation=1,
                mz_service="mz_2",
                system_parameter_defaults=system_parameter_defaults,
            ),
            Manipulate(self, phase=1, mz_service="mz_1"),
            *wait_ready_and_promote("mz_2"),
            Manipulate(self, phase=2, mz_service="mz_2"),
            start_mz_read_only(
                self,
                tag=None,
                deploy_generation=2,
                mz_service="mz_3",
                system_parameter_defaults=system_parameter_defaults,
            ),
            Validate(self, mz_service="mz_2"),
            *wait_ready_and_promote("mz_3"),
            Validate(self, mz_service="mz_3"),
        ]


class ZeroDowntimeBumpedVersion(Scenario):
    """0dt upgrade of the entire Mz instance from the current version to a
    version with just the version number bumped."""

    def actions(self) -> list[Action]:
        system_parameter_defaults = get_default_system_parameters(self.base_version())
        return [
            StartMz(
                self,
                mz_service="mz_1",
                system_parameter_defaults=system_parameter_defaults,
            ),
            Initialize(self, mz_service="mz_1"),
            start_mz_read_only(
                self,
                deploy_generation=1,
                mz_service="mz_2",
                system_parameter_defaults=system_parameter_defaults,
            ),
            Manipulate(self, phase=1, mz_service="mz_1"),
            BumpVersion(),
            *wait_ready_and_promote("mz_2"),
            Manipulate(self, phase=2, mz_service="mz_2"),
            start_mz_read_only(
                self,
                deploy_generation=2,
                mz_service="mz_3",
                system_parameter_defaults=system_parameter_defaults,
                publish=False,  # Allows us to build the image during the test in CI
            ),
            Validate(self, mz_service="mz_2"),
            *wait_ready_and_promote("mz_3"),
            Validate(self, mz_service="mz_3"),
            GitResetHard(),  # Undo the previous version bump in case we need to run the mz container
        ]


class ZeroDowntimeUpgradeEntireMzTwoVersions(Scenario):
    """0dt upgrade of the entire Mz instance starting from the previous
    released version and passing through the last released version."""

    def base_version(self) -> MzVersion:
        return get_previous_version()

    def actions(self) -> list[Action]:
        print(f"Upgrade path: {self.base_version()} -> {get_last_version()} -> current")
        system_parameter_defaults = get_default_system_parameters(self.base_version())
        return [
            # Start with previous_version
            StartMz(
                self,
                tag=self.base_version(),
                mz_service="mz_1",
                system_parameter_defaults=system_parameter_defaults,
            ),
            Initialize(self, mz_service="mz_1"),
            # Upgrade to last_version
            start_mz_read_only(
                self,
                tag=get_last_version(),
                deploy_generation=1,
                mz_service="mz_2",
                system_parameter_defaults=system_parameter_defaults,
            ),
            Manipulate(self, phase=1, mz_service="mz_1"),
            *wait_ready_and_promote("mz_2"),
            # Upgrade to current source
            start_mz_read_only(
                self,
                tag=None,
                deploy_generation=2,
                mz_service="mz_3",
                system_parameter_defaults=system_parameter_defaults,
            ),
            Manipulate(self, phase=2, mz_service="mz_2"),
            *wait_ready_and_promote("mz_3"),
            start_mz_read_only(
                self,
                tag=None,
                deploy_generation=3,
                mz_service="mz_4",
                system_parameter_defaults=system_parameter_defaults,
            ),
            Validate(self, mz_service="mz_3"),
            *wait_ready_and_promote("mz_4"),
            Validate(self, mz_service="mz_4"),
        ]


class ZeroDowntimeUpgradeEntireMzFourVersions(Scenario):
    """Test 0dt upgrade from X-4 -> X-3 -> X-2 -> X-1 -> X"""

    def __init__(
        self,
        checks: list[type[Check]],
        executor: Executor,
        features: Features,
        seed: str | None = None,
    ):
        self.minor_versions = get_minor_versions()
        super().__init__(checks, executor, features, seed)

    def base_version(self) -> MzVersion:
        return self.minor_versions[3]

    def actions(self) -> list[Action]:
        print(
            f"Upgrade path: {self.minor_versions[3]} -> {self.minor_versions[2]} -> {get_previous_version()} -> {get_last_version()} -> current"
        )
        system_parameter_defaults = get_default_system_parameters(self.base_version())
        return [
            StartMz(
                self,
                tag=self.minor_versions[3],
                mz_service="mz_1",
                system_parameter_defaults=system_parameter_defaults,
            ),
            Initialize(self, mz_service="mz_1"),
            start_mz_read_only(
                self,
                tag=self.minor_versions[2],
                deploy_generation=1,
                mz_service="mz_2",
                system_parameter_defaults=system_parameter_defaults,
            ),
            Manipulate(self, phase=1, mz_service="mz_1"),
            *wait_ready_and_promote("mz_2"),
            start_mz_read_only(
                self,
                tag=get_previous_version(),
                deploy_generation=2,
                mz_service="mz_3",
                system_parameter_defaults=system_parameter_defaults,
            ),
            Manipulate(self, phase=2, mz_service="mz_2"),
            *wait_ready_and_promote("mz_3"),
            start_mz_read_only(
                self,
                tag=get_last_version(),
                deploy_generation=3,
                mz_service="mz_4",
                system_parameter_defaults=system_parameter_defaults,
            ),
            Validate(self, mz_service="mz_3"),
            *wait_ready_and_promote("mz_4"),
            start_mz_read_only(
                self,
                tag=None,
                deploy_generation=4,
                mz_service="mz_5",
                system_parameter_defaults=system_parameter_defaults,
            ),
            Validate(self, mz_service="mz_4"),
            *wait_ready_and_promote("mz_5"),
            Validate(self, mz_service="mz_5"),
        ]


def create_zero_downtime_basic(
    name: str,
    base_version: MzVersion,
) -> type[Scenario]:

    def actions(self) -> list[Action]:
        return [
            StartMz(
                self,
                tag=self.base_version(),
                mz_service="mz_1",
                system_parameter_defaults=get_default_system_parameters(
                    self.base_version()
                ),
            ),
            Initialize(self, mz_service="mz_1"),
            Manipulate(self, phase=1, mz_service="mz_1"),
            Manipulate(self, phase=2, mz_service="mz_1"),
            start_mz_read_only(
                self,
                tag=None,
                deploy_generation=1,
                mz_service="mz_2",
                system_parameter_defaults=get_default_system_parameters(
                    self.base_version()
                ),
            ),
            *wait_ready_and_promote("mz_2"),
            Validate(self, mz_service="mz_2"),
        ]

    return type(
        name,
        (Scenario,),
        {
            "base_version": lambda self: base_version,
            "actions": actions,
        },
    )


versions_from_docs = [
    version
    for version in VersionsFromDocs(respect_released_tag=True).minor_versions()
    if version >= MzVersion.parse_mz("v0.126.0")
]


zero_downtime_basic_scenarios = [
    create_zero_downtime_basic(
        name=f"MultiVersionZeroDowntimeBasic_{version}",
        base_version=version,
    )
    for version in versions_from_docs
]
