# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from typing import List, Type

from materialize.mzcompose import Composition
from materialize.platform_upgrade_test.actions import (
    Action,
    Populate,
    StartInstance,
    Validate,
)
from materialize.platform_upgrade_test.checks import Check


class Scenario:
    def __init__(self, checks: List[Type[Check]]) -> None:
        self.checks = checks

    def actions(self) -> List[Action]:
        assert False

    def run(self, c: Composition) -> None:
        for action in self.actions():
            action.execute(c)


class NoRestartNoUpgrade(Scenario):
    def actions(self) -> List[Action]:
        return [StartInstance(), Populate(self.checks), Validate(self.checks)]
