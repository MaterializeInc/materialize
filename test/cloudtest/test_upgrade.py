# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from typing import List, Optional

import pytest

from materialize.checks.actions import Action, Initialize, Manipulate, Validate
from materialize.checks.all_checks import *  # noqa: F401 F403
from materialize.checks.checks import Check
from materialize.checks.cloudtest_actions import ReplaceEnvironmentdStatefulSet
from materialize.checks.executors import CloudtestExecutor
from materialize.checks.scenarios import Scenario
from materialize.cloudtest.app.materialize_application import MaterializeApplication
from materialize.cloudtest.wait import wait
from materialize.util import MzVersion
from materialize.version_list import VersionsFromDocs

LAST_RELEASED_VERSION = VersionsFromDocs().minor_versions()[-1]


class CloudtestUpgrade(Scenario):
    """A Platform Checks scenario that performs an upgrade in cloudtest/K8s"""

    def base_version(self) -> MzVersion:
        return LAST_RELEASED_VERSION

    def actions(self) -> List[Action]:
        return [
            Initialize(self),
            Manipulate(self, phase=1),
            ReplaceEnvironmentdStatefulSet(new_tag=None),
            Manipulate(self, phase=2),
            Validate(self),
        ]


@pytest.mark.long
def test_upgrade(
    aws_region: Optional[str], log_filter: Optional[str], dev: bool
) -> None:
    """Test upgrade from the last released verison to the current source by running all the Platform Checks"""

    mz = MaterializeApplication(
        tag=str(LAST_RELEASED_VERSION),
        aws_region=aws_region,
        log_filter=log_filter,
        release_mode=(not dev),
    )
    wait(condition="condition=Ready", resource="pod/cluster-u1-replica-1-0")

    executor = CloudtestExecutor(application=mz, version=LAST_RELEASED_VERSION)
    scenario = CloudtestUpgrade(checks=Check.__subclasses__(), executor=executor)
    scenario.run()
