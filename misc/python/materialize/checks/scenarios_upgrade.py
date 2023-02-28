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

    def tag(self) -> MzVersion:
        return last_version

    def actions(self) -> List[Action]:
        print(f"Upgrading from tag {self.tag()}")
        return [
            StartMz(tag=self.tag()),
            Initialize(self.checks(), base_version=self.tag()),
            Manipulate(self.checks(), base_version=self.tag(), phase=1),
            KillMz(),
            StartMz(tag=None),
            Manipulate(self.checks(), base_version=self.tag(), phase=2),
            Validate(self.checks(), base_version=self.tag()),
            # A second restart while already on the new version
            KillMz(),
            StartMz(tag=None),
            Validate(self.checks(), base_version=self.tag()),
        ]


class UpgradeEntireMzPreviousVersion(UpgradeEntireMz):
    """Upgrade the entire Mz instance from the previous released version."""

    def tag(self) -> MzVersion:
        return previous_version


#
# We are limited with respect to the different orders in which stuff can be upgraded:
# - some sequences of events are invalid
# - environmentd and storage clusterds are located in the same container
#
# Still, we would like to try as many scenarios as we can
#


class UpgradeClusterdComputeLast(Scenario):
    """Upgrade compute's clusterd separately after upgrading environmentd"""

    def actions(self) -> List[Action]:
        return [
            StartMz(tag=last_version),
            StartClusterdCompute(tag=last_version),
            UseClusterdCompute(base_version=last_version),
            Initialize(self.checks(), base_version=last_version),
            Manipulate(self.checks(), base_version=last_version, phase=1),
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
            Manipulate(self.checks(), base_version=last_version, phase=2),
            Validate(self.checks(), base_version=last_version),
            # A second restart while already on the new version
            KillMz(),
            StartMz(tag=None),
            Validate(self.checks(), base_version=last_version),
        ]


class UpgradeClusterdComputeFirst(Scenario):
    """Upgrade compute's clusterd separately before environmentd"""

    def actions(self) -> List[Action]:
        return [
            StartMz(tag=last_version),
            StartClusterdCompute(tag=last_version),
            UseClusterdCompute(base_version=last_version),
            Initialize(self.checks(), base_version=last_version),
            Manipulate(self.checks(), base_version=last_version, phase=1),
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
            Manipulate(self.checks(), base_version=last_version, phase=2),
            Validate(self.checks(), base_version=last_version),
            KillMz(),
            StartMz(tag=None),
            Validate(self.checks(), base_version=last_version),
        ]
