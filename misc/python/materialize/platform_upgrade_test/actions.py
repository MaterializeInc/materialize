# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from typing import TYPE_CHECKING, List, Type

from materialize.mzcompose import Composition

if TYPE_CHECKING:
    from materialize.platform_upgrade_test.checks import Check


class Action:
    def execute(self, c: Composition) -> None:
        assert False


class StartInstance(Action):
    def execute(self, c: Composition) -> None:
        c.up("materialized")
        c.wait_for_materialized()


class Populate(Action):
    def __init__(self, checks: List[Type["Check"]]) -> None:
        self.checks = [check_class() for check_class in checks]

    def execute(self, c: Composition) -> None:
        for check in self.checks:
            check.run_populate(c)


class Validate(Action):
    def __init__(self, checks: List[Type["Check"]]) -> None:
        self.checks = [check_class() for check_class in checks]

    def execute(self, c: Composition) -> None:
        for check in self.checks:
            check.run_validate(c)


class Testdrive(Action):
    def __init__(self, td_str: str) -> None:
        self.td_str = td_str

    def execute(self, c: Composition) -> None:
        c.testdrive(input=self.td_str)
