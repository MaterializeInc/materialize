# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import time
from typing import TYPE_CHECKING, List, Optional, Type

from materialize.checks.executors import Executor

if TYPE_CHECKING:
    from materialize.checks.checks import Check


class Action:
    def execute(self, e: Executor) -> None:
        assert False


class Testdrive(Action):
    # Instruct pytest this class does not contain actual tests
    __test__ = False

    def __init__(self, input: str) -> None:
        self.input = input

    def execute(self, e: Executor) -> None:
        """Pass testdrive actions to be run by an Executor-specific implementation."""
        e.testdrive(self.input)


class Sleep(Action):
    def __init__(self, interval: float) -> None:
        self.interval = interval

    def execute(self, e: Executor) -> None:
        print(f"Sleeping for {self.interval} seconds")
        time.sleep(self.interval)


class Initialize(Action):
    def __init__(self, checks: List[Type["Check"]]) -> None:
        self.checks = [check_class() for check_class in checks]

    def execute(self, e: Executor) -> None:
        for check in self.checks:
            print(f"Running initialize() from {check}")
            check.run_initialize(e)


class Manipulate(Action):
    def __init__(
        self, checks: List[Type["Check"]], phase: Optional[int] = None
    ) -> None:
        assert phase is not None
        self.phase = phase - 1

        self.checks = [check_class() for check_class in checks]
        assert len(self.checks) >= self.phase

    def execute(self, e: Executor) -> None:
        assert self.phase is not None
        for check in self.checks:
            print(f"Running manipulate() from {check}")
            check.run_manipulate(e, self.phase)


class Validate(Action):
    def __init__(self, checks: List[Type["Check"]]) -> None:
        self.checks = [check_class() for check_class in checks]

    def execute(self, e: Executor) -> None:
        for check in self.checks:
            print(f"Running validate() from {check}")
            check.run_validate(e)
