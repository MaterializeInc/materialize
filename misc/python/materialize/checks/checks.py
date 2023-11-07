# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from random import Random
from typing import Any

from materialize.checks.actions import Testdrive
from materialize.checks.executors import Executor
from materialize.util import MzVersion

TESTDRIVE_NOP = "$ nop"


class Check:
    # Has to be set for the class already, not just in the constructor, so that
    # we can change the value for the entire class in the decorator
    enabled: bool = True

    def __init__(self, base_version: MzVersion, rng: Random | None) -> None:
        self.base_version = base_version
        self.rng = rng

    def _can_run(self, e: Executor) -> bool:
        return True

    def initialize(self) -> Testdrive:
        return Testdrive(TESTDRIVE_NOP)

    def manipulate(self) -> list[Testdrive]:
        assert False

    def validate(self) -> Testdrive:
        assert False

    def start_initialize(self, e: Executor) -> None:
        if self._can_run(e) and self.enabled:
            self.current_version = e.current_mz_version
            self._initialize = self.initialize()
            self._initialize.execute(e)

    def join_initialize(self, e: Executor) -> None:
        if self._can_run(e) and self.enabled:
            self._initialize.join(e)

    def start_manipulate(self, e: Executor, phase: int) -> None:
        if self._can_run(e) and self.enabled:
            self.current_version = e.current_mz_version
            self._manipulate = self.manipulate()
            assert (
                len(self._manipulate) == 2
            ), f"manipulate() should return a list with exactly 2 elements, but actually returns {len(self._manipulate)} elements"
            self._manipulate[phase].execute(e)

    def join_manipulate(self, e: Executor, phase: int) -> None:
        if self._can_run(e) and self.enabled:
            self._manipulate[phase].join(e)

    def start_validate(self, e: Executor) -> None:
        if self._can_run(e) and self.enabled:
            self.current_version = e.current_mz_version
            self._validate = self.validate()
            self._validate.execute(e)

    def join_validate(self, e: Executor) -> None:
        if self._can_run(e) and self.enabled:
            self._validate.join(e)


def disabled(ignore_reason: str):
    class ClassWrapper:
        def __init__(self, cls: type[Check]):
            assert issubclass(cls, Check)
            self.check_class = cls
            self.check_class.enabled = False

        def __call__(self, *cls_ars: Any):
            check = self.check_class(*cls_ars)
            return check


    return ClassWrapper
