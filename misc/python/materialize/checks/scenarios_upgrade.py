# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


from materialize.checks.actions import Action, Initialize, Manipulate, Sleep, Validate
from materialize.checks.checks import Check
from materialize.checks.executors import Executor
from materialize.checks.mzcompose_actions import (
    KillClusterdCompute,
    KillMz,
    StartClusterdCompute,
    StartMz,
    UseClusterdCompute,
)
from materialize.checks.scenarios import Scenario
from materialize.mz_version import MzVersion
from materialize.version_list import get_published_minor_mz_versions

# late initialization
_minor_versions: list[MzVersion] | None = None
_last_version: MzVersion | None = None
_previous_version: MzVersion | None = None


def get_minor_versions() -> list[MzVersion]:
    global _minor_versions
    if _minor_versions is None:
        _minor_versions = get_published_minor_mz_versions(limit=4)
    return _minor_versions


def get_last_version() -> MzVersion:
    global _last_version
    if _last_version is None:
        _last_version = get_minor_versions()[0]
    return _last_version


def get_previous_version() -> MzVersion:
    global _previous_version
    if _previous_version is None:
        _previous_version = get_minor_versions()[1]
    return _previous_version


class UpgradeEntireMz(Scenario):
    """Upgrade the entire Mz instance from the last released version."""

    def base_version(self) -> MzVersion:
        return get_last_version()

    def actions(self) -> list[Action]:
        print(f"Upgrading from tag {self.base_version()}")
        return [
            StartMz(self, tag=self.base_version()),
            Initialize(self),
            Manipulate(self, phase=1),
            KillMz(),
            StartMz(self, tag=None),
            Manipulate(self, phase=2),
            Validate(self),
            # A second restart while already on the new version
            KillMz(),
            StartMz(self, tag=None),
            Validate(self),
        ]


class UpgradeEntireMzPreviousVersion(UpgradeEntireMz):
    """Upgrade the entire Mz instance from the previous released version."""

    def base_version(self) -> MzVersion:
        return get_previous_version()


class UpgradeEntireMzTwoVersions(Scenario):
    """Upgrade the entire Mz instance starting from the previous
    released version and passing through the last released version."""

    def base_version(self) -> MzVersion:
        return get_previous_version()

    def actions(self) -> list[Action]:
        print(
            f"Upgrading starting from tag {self.base_version()} going through {get_last_version()}"
        )
        return [
            # Start with previous_version
            StartMz(self, tag=self.base_version()),
            Initialize(self),
            # Upgrade to last_version
            KillMz(),
            StartMz(self, tag=get_last_version()),
            Manipulate(self, phase=1),
            # Upgrade to current source
            KillMz(),
            StartMz(self, tag=None),
            Manipulate(self, phase=2),
            Validate(self),
            # A second restart while already on the current source
            KillMz(),
            StartMz(self, tag=None),
            Validate(self),
        ]


class UpgradeEntireMzSkipVersion(Scenario):
    """Upgrade the entire Mz instance from the previous version directly to the current HEAD"""

    def base_version(self) -> MzVersion:
        return get_previous_version()

    def actions(self) -> list[Action]:
        print(f"Upgrading starting from tag {self.base_version()} directly to HEAD")
        return [
            # Start with previous_version
            StartMz(self, tag=get_previous_version()),
            Initialize(self),
            Manipulate(self, phase=1),
            # Upgrade directly to current source
            KillMz(),
            StartMz(self, tag=None),
            Manipulate(self, phase=2),
            Validate(self),
            # A second restart while already on the current source
            KillMz(),
            StartMz(self, tag=None),
            Validate(self),
        ]


class UpgradeEntireMzFourVersions(Scenario):
    """Test upgrade X-4 -> X-3 -> X-2 -> X-1 -> X"""

    def __init__(
        self, checks: list[type[Check]], executor: Executor, seed: str | None = None
    ):
        super().__init__(checks, executor, seed)
        self.minor_versions = get_minor_versions()

    def base_version(self) -> MzVersion:
        return self.minor_versions[3]

    def actions(self) -> list[Action]:
        print(
            f"Upgrading going through {self.minor_versions[3]} -> {self.minor_versions[2]} -> {get_previous_version()} -> {get_last_version()}"
        )
        return [
            StartMz(self, tag=self.minor_versions[3]),
            Initialize(self),
            KillMz(),
            StartMz(self, tag=self.minor_versions[2]),
            Manipulate(self, phase=1),
            KillMz(),
            StartMz(self, tag=get_previous_version()),
            Manipulate(self, phase=2),
            KillMz(),
            StartMz(self, tag=get_last_version()),
            KillMz(),
            StartMz(self, tag=None),
            Validate(self),
            KillMz(),
            StartMz(self, tag=None),
            Validate(self),
        ]


#
# We are limited with respect to the different orders in which stuff can be upgraded:
# - some sequences of events are invalid
# - environmentd and storage clusterds are located in the same container
#
# Still, we would like to try as many scenarios as we can
#


class UpgradeClusterdComputeLast(Scenario):
    """Upgrade compute's clusterd separately after upgrading environmentd"""

    def base_version(self) -> MzVersion:
        return get_last_version()

    def actions(self) -> list[Action]:
        return [
            StartMz(self, tag=self.base_version()),
            StartClusterdCompute(tag=self.base_version()),
            UseClusterdCompute(self),
            Initialize(self),
            Manipulate(self, phase=1),
            KillMz(),
            StartMz(self, tag=None),
            # No useful work can be done while clusterd is old-version
            # and environmentd is new-version. So we proceed
            # to upgrade clusterd as well.
            # We sleep here to allow some period of coexistence, even
            # though we are not issuing queries during that time.
            Sleep(10),
            KillClusterdCompute(),
            StartClusterdCompute(tag=None),
            Manipulate(self, phase=2),
            Validate(self),
            # A second restart while already on the new version
            KillMz(),
            StartMz(self, tag=None),
            Validate(self),
        ]


class UpgradeClusterdComputeFirst(Scenario):
    """Upgrade compute's clusterd separately before environmentd"""

    def base_version(self) -> MzVersion:
        return get_last_version()

    def actions(self) -> list[Action]:
        return [
            StartMz(self, tag=self.base_version()),
            StartClusterdCompute(tag=self.base_version()),
            UseClusterdCompute(self),
            Initialize(self),
            Manipulate(self, phase=1),
            KillClusterdCompute(),
            StartClusterdCompute(tag=None),
            # No useful work can be done while clusterd is new-version
            # and environmentd is old-version. So we
            # proceed to upgrade them as well.
            # We sleep here to allow some period of coexistence, even
            # though we are not issuing queries during that time.
            Sleep(10),
            KillMz(),
            StartMz(self, tag=None),
            Manipulate(self, phase=2),
            Validate(self),
            KillMz(),
            StartMz(self, tag=None),
            Validate(self),
        ]
