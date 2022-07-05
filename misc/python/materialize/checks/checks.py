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
from materialize.mzcompose import Composition


class Check:
    def __init__(self) -> None:
        self._initialize = self.initialize()
        self._manipulate = self.manipulate()
        self._validate = self.validate()

    def initialize(self) -> Testdrive:
        return Testdrive("")

    def manipulate(self) -> List[Testdrive]:
        assert False

    def validate(self) -> Testdrive:
        assert False

    def run_initialize(self, c: Composition) -> None:
        self._initialize.execute(c)

    def run_manipulate(self, c: Composition, phase: int) -> None:
        self._manipulate[phase].execute(c)

    def run_validate(self, c: Composition) -> None:
        self._validate.execute(c)


class CheckDisabled(Check):
    def manipulate(self) -> List[Testdrive]:
        return [Testdrive(""), Testdrive("")]

    def validate(self) -> Testdrive:
        return Testdrive("")
