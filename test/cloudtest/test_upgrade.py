# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from textwrap import dedent
from typing import List, Optional

import pytest

from materialize.checks.actions import Action, Initialize, Manipulate, Validate
from materialize.checks.all_checks import *  # noqa: F401 F403
from materialize.checks.checks import Check
from materialize.checks.executors import CloudtestExecutor, Executor
from materialize.checks.scenarios import Scenario
from materialize.cloudtest.application import MaterializeApplication
from materialize.cloudtest.k8s.environmentd import EnvironmentdStatefulSet
from materialize.cloudtest.wait import wait

# This will 'upgrade' from the current source.
# Once a stable release is out, we need to get its DockerHub tag in here
LAST_RELEASED_VERSION = None


class ReplaceEnvironmentdStatefulSet(Action):
    """Change the image tag of the environmentd stateful set, re-create the definition and replace the existing one."""

    def __init__(self, new_tag: Optional[str] = None) -> None:
        self.new_tag = new_tag

    def execute(self, e: Executor) -> None:
        mz = e.cloudtest_application()
        stateful_set = [
            resource
            for resource in mz.resources
            if type(resource) == EnvironmentdStatefulSet
        ]
        assert len(stateful_set) == 1
        stateful_set = stateful_set[0]

        stateful_set.tag = self.new_tag
        stateful_set.replace()


class LiftClusterLimits(Action):
    def execute(self, e: Executor) -> None:
        e.testdrive(
            input=dedent(
                """
                $ postgres-connect name=mz_system url=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}

                $ postgres-execute connection=mz_system
                ALTER SYSTEM SET max_tables = 100;

                ALTER SYSTEM SET max_sources = 100;

                ALTER SYSTEM SET max_materialized_views = 100;
                ALTER SYSTEM SET max_objects_per_schema = 1000;
                """
            )
        )


class CloudtestUpgrade(Scenario):
    """A Platform Checks scenario that performs an upgrade in cloudtest/K8s"""

    def actions(self) -> List[Action]:
        return [
            LiftClusterLimits(),
            Initialize(self.checks),
            Manipulate(self.checks, phase=1),
            ReplaceEnvironmentdStatefulSet(new_tag=None),
            Manipulate(self.checks, phase=2),
            Validate(self.checks),
        ]


@pytest.mark.long
def test_upgrade() -> None:
    """Test upgrade from LAST_RELEASED_VERSION to the current source by running all the Platform Checks"""

    mz = MaterializeApplication(tag=LAST_RELEASED_VERSION)
    wait(condition="condition=Ready", resource="pod/compute-cluster-u1-replica-1-0")

    executor = CloudtestExecutor(application=mz)
    scenario = CloudtestUpgrade(checks=Check.__subclasses__(), executor=executor)
    scenario.run()
