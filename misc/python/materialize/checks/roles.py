# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from textwrap import dedent
from typing import List

from materialize.checks.actions import Testdrive
from materialize.checks.checks import Check
from materialize.util import MzVersion


class CreateRole(Check):
    def _can_run(self) -> bool:
        return self.base_version >= MzVersion.parse("0.45.0-dev")

    def initialize(self) -> Testdrive:
        return Testdrive("")

    def manipulate(self) -> List[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                > CREATE ROLE create_role1;
                """,
                """
                > CREATE ROLE create_role2;
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > SELECT name FROM mz_roles WHERE name LIKE 'create_role%';
                create_role1
                create_role2
            """
            )
        )


class DropRole(Check):
    def _can_run(self) -> bool:
        return self.base_version >= MzVersion.parse("0.45.0-dev")

    def initialize(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > CREATE ROLE drop_role1;
            """
            )
        )

    def manipulate(self) -> List[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                > DROP ROLE drop_role1;
                > CREATE ROLE drop_role2;
                """,
                """
                > DROP ROLE drop_role2;
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > SELECT COUNT(*) FROM mz_roles WHERE name LIKE 'drop_role%';
                0
            """
            )
        )
