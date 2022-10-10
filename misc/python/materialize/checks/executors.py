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

from materialize.cloudtest.application import MaterializeApplication
from materialize.mzcompose import Composition

# from materialize.cloudtest import


class Executor:
    pass

    def testdrive(self, input: str) -> None:
        assert False

    def mzcompose_composition(self) -> Composition:
        assert False

    def cloudtest_application(self) -> MaterializeApplication:
        assert False


class MzcomposeExecutor(Executor):
    def __init__(self, composition: Composition) -> None:
        self.composition = composition

    def mzcompose_composition(self) -> Composition:
        return self.composition

    def testdrive(self, input: str) -> None:
        self.composition.testdrive(input)


class CloudtestExecutor(Executor):
    def __init__(self, application: MaterializeApplication) -> None:
        self.application = application

    def cloudtest_application(self) -> MaterializeApplication:
        return self.application

    def testdrive(self, input: str) -> None:
        self.application.testdrive.run(input=input, no_reset=True, seed=1)
