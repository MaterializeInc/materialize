# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import textwrap
import time
from inspect import getframeinfo, stack
from typing import TYPE_CHECKING, Any

from materialize import MZ_ROOT, spawn
from materialize.checks.executors import Executor
from materialize.mz_version import MzVersion

if TYPE_CHECKING:
    from materialize.checks.scenarios import Scenario


class Action:
    def __init__(self) -> None:
        self.mz_service = None
        self.phase = None

    def execute(self, e: Executor) -> None:
        raise NotImplementedError

    def join(self, e: Executor) -> None:
        print(f"Action {self} does not implement join()")
        raise NotImplementedError


class Testdrive(Action):
    # Instruct pytest this class does not contain actual tests
    __test__ = False

    def __init__(self, input: str, dedent: bool = True) -> None:
        self.input = textwrap.dedent(input) if dedent else input
        self.handle: Any | None = None
        self.caller = getframeinfo(stack()[1][0])

    def execute(self, e: Executor, mz_service: str | None = None) -> None:
        """Pass testdrive actions to be run by an Executor-specific implementation."""
        self.handle = e.testdrive(self.input, self.caller, mz_service)

    def join(self, e: Executor) -> None:
        e.join(self.handle)


class Sleep(Action):
    def __init__(self, interval: float) -> None:
        self.interval = interval

    def execute(self, e: Executor) -> None:
        print(f"Sleeping for {self.interval} seconds")
        time.sleep(self.interval)

    def join(self, e: Executor) -> None:
        pass


class Initialize(Action):
    def __init__(self, scenario: "Scenario", mz_service: str | None = None) -> None:
        self.checks = scenario.check_objects
        self.mz_service = mz_service

    def execute(self, e: Executor) -> None:
        for check in self.checks:
            print(f"Running initialize() from {check}")
            check.start_initialize(e, self)

    def join(self, e: Executor) -> None:
        for check in self.checks:
            check.join_initialize(e)


class Manipulate(Action):
    def __init__(
        self,
        scenario: "Scenario",
        phase: int | None = None,
        mz_service: str | None = None,
    ) -> None:
        assert phase is not None
        self.phase = phase - 1
        self.mz_service = mz_service

        self.checks = scenario.check_objects

    def execute(self, e: Executor) -> None:
        assert self.phase is not None
        for check in self.checks:
            print(f"Running manipulate() from {check}")
            check.start_manipulate(e, self)

    def join(self, e: Executor) -> None:
        assert self.phase is not None
        for check in self.checks:
            check.join_manipulate(e, self)


class Validate(Action):
    def __init__(self, scenario: "Scenario", mz_service: str | None = None) -> None:
        self.checks = scenario.check_objects
        self.mz_service = mz_service

    def execute(self, e: Executor) -> None:
        for check in self.checks:
            print(f"Running validate() from {check}")
            check.start_validate(e, self)

    def join(self, e: Executor) -> None:
        for check in self.checks:
            check.join_validate(e)


class BumpVersion(Action):
    def execute(self, e: Executor) -> None:
        version = MzVersion.parse_cargo().bump_minor()
        spawn.runv(["bin/bump-version", str(version), "--no-commit"], cwd=MZ_ROOT)

    def join(self, e: Executor) -> None:
        pass


class GitResetHard(Action):
    def execute(self, e: Executor) -> None:
        MzVersion.parse_cargo().bump_minor()
        spawn.runv(["git", "reset", "--hard"], cwd=MZ_ROOT)

    def join(self, e: Executor) -> None:
        pass
