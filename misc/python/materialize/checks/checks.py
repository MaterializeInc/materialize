# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from random import Random
from typing import List, Optional

from materialize.checks.actions import Testdrive
from materialize.checks.executors import Executor
from materialize.util import MzVersion


class Check:
    def __init__(self, base_version: MzVersion, rng: Optional[Random]) -> None:
        self.base_version = base_version
        self.rng = rng

    def _can_run(self) -> bool:
        return True

    def initialize(self) -> Testdrive:
        return Testdrive("")

    def manipulate(self) -> List[Testdrive]:
        assert False

    def validate(self) -> Testdrive:
        assert False

    def start_initialize(self, e: Executor) -> None:
        if self._can_run():
            self.current_version = e.current_mz_version
            self._initialize = self.initialize()
            self._initialize.execute(e)

    def join_initialize(self, e: Executor) -> None:
        if self._can_run():
            self._initialize.join(e)

    def start_manipulate(self, e: Executor, phase: int) -> None:
        if self._can_run():
            self.current_version = e.current_mz_version
            self._manipulate = self.manipulate()
            assert (
                len(self._manipulate) == 2
            ), f"manipulate() should return a list with exactly 2 elements, but actually returns {len(self._manipulate)} elements"
            self._manipulate[phase].execute(e)

    def join_manipulate(self, e: Executor, phase: int) -> None:
        if self._can_run():
            self._manipulate[phase].join(e)

    def start_validate(self, e: Executor) -> None:
        if self._can_run():
            self.current_version = e.current_mz_version
            self._validate = self.validate()
            self._validate.execute(e)

    def join_validate(self, e: Executor) -> None:
        if self._can_run():
            self._validate.join(e)


class CheckDisabled(Check):
    def manipulate(self) -> List[Testdrive]:
        return [Testdrive(""), Testdrive("")]

    def validate(self) -> Testdrive:
        return Testdrive("")
