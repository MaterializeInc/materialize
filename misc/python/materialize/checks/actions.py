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

from typing import TYPE_CHECKING, List, Optional, Type

from materialize.mzcompose import Composition
from materialize.mzcompose.services import Computed

if TYPE_CHECKING:
    from materialize.checks.checks import Check


class Action:
    def execute(self, c: Composition) -> None:
        assert False


class StartMz(Action):
    def execute(self, c: Composition) -> None:
        c.up("materialized")
        c.wait_for_materialized()


class UseComputed(Action):
    def execute(self, c: Composition) -> None:
        c.sql(
            """
            DROP CLUSTER REPLICA default.default_replica;
            CREATE CLUSTER REPLICA default.default_replica REMOTE ['computed_1:2100'];
        """
        )


class KillComputed(Action):
    def execute(self, c: Composition) -> None:
        with c.override(Computed(name="computed_1")):
            c.kill("computed_1")


class StartComputed(Action):
    def execute(self, c: Composition) -> None:
        with c.override(
            Computed(
                name="computed_1", options="--workers 1 --process 0 computed_1:2102"
            )
        ):
            c.up("computed_1")


class RestartMz(Action):
    def execute(self, c: Composition) -> None:
        c.kill("materialized")
        c.up("materialized")
        c.wait_for_materialized()


class DropCreateDefaultReplica(Action):
    def execute(self, c: Composition) -> None:
        c.sql(
            """
           DROP CLUSTER REPLICA default.default_replica;
           CREATE CLUSTER REPLICA default.default_replica SIZE '1';
        """
        )


class Initialize(Action):
    def __init__(self, checks: List[Type["Check"]]) -> None:
        self.checks = [check_class() for check_class in checks]

    def execute(self, c: Composition) -> None:
        for check in self.checks:
            print(f"Running initialize() from {check}")
            check.run_initialize(c)


class Manipulate(Action):
    def __init__(
        self, checks: List[Type["Check"]], phase: Optional[int] = None
    ) -> None:
        assert phase is not None
        self.phase = phase - 1

        self.checks = [check_class() for check_class in checks]
        assert len(self.checks) >= self.phase

    def execute(self, c: Composition) -> None:
        assert self.phase is not None
        for check in self.checks:
            print(f"Running manipulate() from {check}")
            check.run_manipulate(c, self.phase)


class Validate(Action):
    def __init__(self, checks: List[Type["Check"]]) -> None:
        self.checks = [check_class() for check_class in checks]

    def execute(self, c: Composition) -> None:
        for check in self.checks:
            print(f"Running validate() from {check}")
            check.run_validate(c)


class Testdrive(Action):
    def __init__(self, td_str: str) -> None:
        self.td_str = td_str

    def execute(self, c: Composition) -> None:
        c.testdrive(input=self.td_str)
