# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import time
from typing import TYPE_CHECKING, Any, Optional

from materialize.checks.executors import Executor

if TYPE_CHECKING:
    from materialize.checks.scenarios import Scenario


class Action:
    def execute(self, e: Executor) -> None:
        assert False

    def join(self, e: Executor) -> None:
        assert False


class Testdrive(Action):
    # Instruct pytest this class does not contain actual tests
    __test__ = False

    def __init__(self, input: str) -> None:
        self.input = input
        self.handle: Optional[Any] = None

    def execute(self, e: Executor) -> None:
        """Pass testdrive actions to be run by an Executor-specific implementation."""
        self.handle = e.testdrive(self.input)

    def join(self, e: Executor) -> None:
        e.join(self.handle)


class Sleep(Action):
    def __init__(self, interval: float) -> None:
        self.interval = interval

    def execute(self, e: Executor) -> None:
        print(f"Sleeping for {self.interval} seconds")
        time.sleep(self.interval)


class Initialize(Action):
    def __init__(self, scenario: "Scenario") -> None:
        self.checks = [
            check_class(scenario.base_version()) for check_class in scenario.checks()
        ]

    def execute(self, e: Executor) -> None:
        for check in self.checks:
            print(f"Running initialize() from {check}")
            check.start_initialize(e)

        for check in self.checks:
            check.join_initialize(e)


class Manipulate(Action):
    def __init__(
        self,
        scenario: "Scenario",
        phase: Optional[int] = None,
    ) -> None:
        assert phase is not None
        self.phase = phase - 1

        self.checks = [
            check_class(scenario.base_version()) for check_class in scenario.checks()
        ]
        assert len(self.checks) >= self.phase

    def execute(self, e: Executor) -> None:
        assert self.phase is not None
        for check in self.checks:
            print(f"Running manipulate() from {check}")
            check.start_manipulate(e, self.phase)

        for check in self.checks:
            check.join_manipulate(e, self.phase)


class Validate(Action):
    def __init__(self, scenario: "Scenario") -> None:
        self.checks = [
            check_class(scenario.base_version()) for check_class in scenario.checks()
        ]

    def execute(self, e: Executor) -> None:
        for check in self.checks:
            print(f"Running validate() from {check}")
            check.start_validate(e)

        for check in self.checks:
            check.join_validate(e)
