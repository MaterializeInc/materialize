# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import random
import threading
from inspect import Traceback
from typing import Any, Optional, Set

from materialize.cloudtest.app.materialize_application import MaterializeApplication
from materialize.mzcompose import Composition
from materialize.util import MzVersion


class Executor:
    # Store the current Materialize version and keep it up-to-date during
    # upgrades so that actions can depend not just on the base version, but
    # also on the current version. This enables testing more interesting
    # scenarios which are still in development and not available a few versions
    # back already.
    current_mz_version: MzVersion
    # All the system settings we have already set in previous Mz versions. No
    # need to set them again in a future version since they should be
    # persisted.
    system_settings: Set[str] = set()

    def testdrive(self, input: str, caller: Optional[Traceback] = None) -> Any:
        assert False

    def mzcompose_composition(self) -> Composition:
        assert False

    def cloudtest_application(self) -> MaterializeApplication:
        assert False

    def join(self, handle: Any) -> None:
        pass


class MzcomposeExecutor(Executor):
    def __init__(self, composition: Composition) -> None:
        self.composition = composition

    def mzcompose_composition(self) -> Composition:
        return self.composition

    def testdrive(self, input: str, caller: Optional[Traceback] = None) -> None:
        self.composition.testdrive(input, caller=caller)


class MzcomposeExecutorParallel(MzcomposeExecutor):
    def __init__(self, composition: Composition) -> None:
        self.composition = composition
        self.exception: Optional[BaseException] = None

    def testdrive(self, input: str, caller: Optional[Traceback] = None) -> Any:
        thread = threading.Thread(target=self._testdrive, args=[input, caller])
        thread.start()
        return thread

    def _testdrive(self, input: str, caller: Optional[Traceback] = None) -> None:
        try:
            self.composition.testdrive(input, caller=caller)
        except BaseException as e:
            self.exception = e

    def join(self, handle: Any) -> None:
        assert type(handle) is threading.Thread
        handle.join()
        if self.exception:
            raise self.exception


class CloudtestExecutor(Executor):
    def __init__(self, application: MaterializeApplication, version: MzVersion) -> None:
        self.application = application
        self.seed = random.getrandbits(32)
        self.current_mz_version = version

    def cloudtest_application(self) -> MaterializeApplication:
        return self.application

    def testdrive(self, input: str, caller: Optional[Traceback] = None) -> None:
        self.application.testdrive.run(
            input=input, no_reset=True, seed=self.seed, caller=caller
        )
