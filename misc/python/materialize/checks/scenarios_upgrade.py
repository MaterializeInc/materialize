# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from typing import List

from materialize.checks.actions import Action, Initialize, Manipulate, Sleep, Validate
from materialize.checks.mzcompose_actions import (
    KillClusterdCompute,
    KillMz,
    StartClusterdCompute,
    StartMz,
    UseClusterdCompute,
)
from materialize.checks.scenarios import Scenario
from materialize.util import MzVersion, released_materialize_versions

released_versions = released_materialize_versions()

# Usually, the latest patch version of the current release
last_version = released_versions[0]

# Usually, the last patch version of the previous release
previous_version = released_versions[1]


class UpgradeEntireMz(Scenario):
    """Upgrade the entire Mz instance from the last released version."""

    def base_version(self) -> MzVersion:
        return last_version

    def actions(self) -> List[Action]:
        print(f"Upgrading from tag {self.base_version()}")
        return [
            StartMz(tag=self.base_version()),
            Initialize(self),
            Manipulate(self, phase=1),
            KillMz(),
            StartMz(tag=None),
            Manipulate(self, phase=2),
            Validate(self),
            # A second restart while already on the new version
            KillMz(),
            StartMz(tag=None),
            Validate(self),
        ]


class UpgradeEntireMzPreviousVersion(UpgradeEntireMz):
    """Upgrade the entire Mz instance from the previous released version."""

    def base_version(self) -> MzVersion:
        return previous_version


class UpgradeEntireMzTwoVersions(Scenario):
    """Upgrade the entire Mz instance starting from the previous
    released version and passing through the last released version."""

    def base_version(self) -> MzVersion:
        return previous_version

    def actions(self) -> List[Action]:
        print(
            f"Upgrading starting from tag {self.base_version()} going through {last_version}"
        )
        return [
            # Start with previous_version
            StartMz(tag=self.base_version()),
            Initialize(self),
            # Upgrade to last_version
            KillMz(),
            StartMz(tag=last_version),
            Manipulate(self, phase=1),
            # Upgrade to current source
            KillMz(),
            StartMz(tag=None),
            Manipulate(self, phase=2),
            Validate(self),
            # A second restart while already on the current source
            KillMz(),
            StartMz(tag=None),
            Validate(self),
        ]


class UpgradeEntireMzSkipVersion(Scenario):
    """Upgrade the entire Mz instance from the previous version directly to the current HEAD"""

    def base_version(self) -> MzVersion:
        return previous_version

    def actions(self) -> List[Action]:
        print(f"Upgrading starting from tag {self.base_version()} directly to HEAD")
        return [
            # Start with previous_version
            StartMz(tag=previous_version),
            Initialize(self),
            Manipulate(self, phase=1),
            # Upgrade directly to current source
            KillMz(),
            StartMz(tag=None),
            Manipulate(self, phase=2),
            Validate(self),
            # A second restart while already on the current source
            KillMz(),
            StartMz(tag=None),
            Validate(self),
        ]


class UpgradeEntireMzFourVersions(Scenario):
    """Test upgrade X-4 -> X-3 -> X-2 -> X-1 -> X"""

    def base_version(self) -> MzVersion:
        return released_versions[3]

    def actions(self) -> List[Action]:
        print(
            f"Upgrading going through {released_versions[3]} -> {released_versions[2]} -> {released_versions[1]} -> {released_versions[0]}"
        )
        return [
            StartMz(tag=released_versions[3]),
            Initialize(self),
            KillMz(),
            StartMz(tag=released_versions[2]),
            Manipulate(self, phase=1),
            KillMz(),
            StartMz(tag=released_versions[1]),
            Manipulate(self, phase=2),
            KillMz(),
            StartMz(tag=released_versions[0]),
            KillMz(),
            StartMz(tag=None),
            Validate(self),
            KillMz(),
            StartMz(tag=None),
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
        return last_version

    def actions(self) -> List[Action]:
        return [
            StartMz(tag=self.base_version()),
            StartClusterdCompute(tag=self.base_version()),
            UseClusterdCompute(self),
            Initialize(self),
            Manipulate(self, phase=1),
            KillMz(),
            StartMz(tag=None),
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
            StartMz(tag=None),
            Validate(self),
        ]


class UpgradeClusterdComputeFirst(Scenario):
    """Upgrade compute's clusterd separately before environmentd"""

    def base_version(self) -> MzVersion:
        return last_version

    def actions(self) -> List[Action]:
        return [
            StartMz(tag=self.base_version()),
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
            StartMz(tag=None),
            Manipulate(self, phase=2),
            Validate(self),
            KillMz(),
            StartMz(tag=None),
            Validate(self),
        ]
