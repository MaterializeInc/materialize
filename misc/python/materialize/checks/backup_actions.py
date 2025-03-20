# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


from materialize.checks.actions import Action
from materialize.checks.executors import Executor


class Backup(Action):
    def execute(self, e: Executor) -> None:
        c = e.mzcompose_composition()
        c.backup()

    def join(self, e: Executor) -> None:
        # Action is blocking
        pass


class Restore(Action):
    def execute(self, e: Executor) -> None:
        c = e.mzcompose_composition()
        c.restore()

    def join(self, e: Executor) -> None:
        # Action is blocking
        pass
