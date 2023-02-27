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

from typing import List

from materialize.checks.actions import Testdrive
from materialize.checks.executors import Executor
from materialize.util import MzVersion


class Check:
    def __init__(self, base_version: MzVersion) -> None:
        self.base_version = base_version
        self._initialize = self.initialize()
        self._manipulate = self.manipulate()
        self._validate = self.validate()

    def initialize(self) -> Testdrive:
        return Testdrive("")

    def manipulate(self) -> List[Testdrive]:
        assert False

    def validate(self) -> Testdrive:
        assert False

    def start_initialize(self, e: Executor) -> None:
        self._initialize.execute(e)

    def join_initialize(self, e: Executor) -> None:
        self._initialize.join(e)

    def start_manipulate(self, e: Executor, phase: int) -> None:
        self._manipulate[phase].execute(e)

    def join_manipulate(self, e: Executor, phase: int) -> None:
        self._manipulate[phase].join(e)

    def start_validate(self, e: Executor) -> None:
        self._validate.execute(e)

    def join_validate(self, e: Executor) -> None:
        self._validate.join(e)


class CheckDisabled(Check):
    def manipulate(self) -> List[Testdrive]:
        return [Testdrive(""), Testdrive("")]

    def validate(self) -> Testdrive:
        return Testdrive("")
