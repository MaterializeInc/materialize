# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


from materialize.checks.actions import Action, Initialize, Manipulate, Validate
from materialize.checks.checks import Check
from materialize.checks.executors import Executor
from materialize.checks.mzcompose_actions import (
    KillMz,
    PromoteMz,
    StartMz,
    WaitReadyMz,
)
from materialize.checks.scenarios import Scenario
from materialize.checks.scenarios_upgrade import (
    get_last_version,
    get_minor_versions,
    get_previous_version,
)
from materialize.mz_version import MzVersion


def start_mz_in_read_only_and_deploy(
    scenario: Scenario,
    deploy_generation: int,
    mz_service: str = "materialized",
    tag: MzVersion | None = None,
) -> list[Action]:
    result: list[Action] = []
    result.extend(
        [
            StartMz(
                scenario,
                tag=tag,
                mz_service=mz_service,
                deploy_generation=deploy_generation,
                healthcheck=[
                    "CMD",
                    "curl",
                    "-f",
                    "localhost:6878/api/leader/status",
                ],
            ),
            WaitReadyMz(mz_service=mz_service),
            PromoteMz(mz_service=mz_service),
        ]
    )
    return result


class ZeroDowntimeRestartEntireMz(Scenario):
    def actions(self) -> list[Action]:
        return [
            StartMz(self, mz_service="mz_1"),
            Initialize(self, mz_service="mz_1"),
            *start_mz_in_read_only_and_deploy(
                self, deploy_generation=1, mz_service="mz_2"
            ),
            Manipulate(self, phase=1, mz_service="mz_2"),
            *start_mz_in_read_only_and_deploy(
                self, deploy_generation=2, mz_service="mz_1"
            ),
            Manipulate(self, phase=2, mz_service="mz_1"),
            *start_mz_in_read_only_and_deploy(
                self, deploy_generation=3, mz_service="mz_2"
            ),
            Validate(self, mz_service="mz_2"),
        ]


class ZeroDowntimeUpgradeEntireMz(Scenario):
    """0dt upgrade of the entire Mz instance from the last released version."""

    def base_version(self) -> MzVersion:
        return get_last_version()

    def actions(self) -> list[Action]:
        print(f"Upgrading from tag {self.base_version()}")
        return [
            StartMz(self, tag=self.base_version(), mz_service="mz_1"),
            Initialize(self, mz_service="mz_1"),
            Manipulate(self, phase=1, mz_service="mz_1"),
            *start_mz_in_read_only_and_deploy(
                self, deploy_generation=1, mz_service="mz_2"
            ),
            Manipulate(self, phase=2, mz_service="mz_2"),
            Validate(self, mz_service="mz_2"),
            # A second restart while already on the new version
            KillMz(capture_logs=True, mz_service="mz_2"),
            StartMz(self, tag=None, mz_service="mz_2", deploy_generation=1),
            Validate(self, mz_service="mz_2"),
        ]


class ZeroDowntimeUpgradeEntireMzTwoVersions(Scenario):
    """0dt upgrade of the entire Mz instance starting from the previous
    released version and passing through the last released version."""

    def base_version(self) -> MzVersion:
        return get_previous_version()

    def actions(self) -> list[Action]:
        print(f"Upgrade path: {self.base_version()} -> {get_last_version()} -> current")
        return [
            # Start with previous_version
            StartMz(self, tag=self.base_version(), mz_service="mz_1"),
            Initialize(self, mz_service="mz_1"),
            # Upgrade to last_version
            *start_mz_in_read_only_and_deploy(
                self, deploy_generation=1, mz_service="mz_2"
            ),
            Manipulate(self, phase=1, mz_service="mz_2"),
            # Upgrade to current source
            *start_mz_in_read_only_and_deploy(
                self, deploy_generation=2, mz_service="mz_1"
            ),
            Manipulate(self, phase=2, mz_service="mz_1"),
            Validate(self, mz_service="mz_1"),
            # A second restart while already on the current source
            KillMz(mz_service="mz_1"),
            StartMz(self, tag=None, mz_service="mz_1", deploy_generation=2),
            Validate(self, mz_service="mz_1"),
        ]


class ZeroDowntimeUpgradeEntireMzFourVersions(Scenario):
    """Test 0dt upgrade from X-4 -> X-3 -> X-2 -> X-1 -> X"""

    def __init__(
        self, checks: list[type[Check]], executor: Executor, seed: str | None = None
    ):
        self.minor_versions = get_minor_versions()
        super().__init__(checks, executor, seed)

    def base_version(self) -> MzVersion:
        return self.minor_versions[3]

    def actions(self) -> list[Action]:
        print(
            f"Upgrade path: {self.minor_versions[3]} -> {self.minor_versions[2]} -> {get_previous_version()} -> {get_last_version()} -> current"
        )
        return [
            StartMz(self, tag=self.minor_versions[3], mz_service="mz_1"),
            Initialize(self, mz_service="mz_1"),
            *start_mz_in_read_only_and_deploy(
                self, tag=self.minor_versions[2], deploy_generation=1, mz_service="mz_2"
            ),
            Manipulate(self, phase=1, mz_service="mz_2"),
            *start_mz_in_read_only_and_deploy(
                self, tag=get_previous_version(), deploy_generation=2, mz_service="mz_1"
            ),
            Manipulate(self, phase=2, mz_service="mz_1"),
            *start_mz_in_read_only_and_deploy(
                self, tag=get_last_version(), deploy_generation=3, mz_service="mz_2"
            ),
            *start_mz_in_read_only_and_deploy(
                self, tag=None, deploy_generation=4, mz_service="mz_1"
            ),
            Validate(self, mz_service="mz_1"),
            KillMz(mz_service="mz_1"),
            StartMz(self, tag=None, mz_service="mz_1", deploy_generation=4),
            Validate(self, mz_service="mz_1"),
        ]
