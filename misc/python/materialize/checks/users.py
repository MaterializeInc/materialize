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


class CreateUser(Check):
    def initialize(self) -> Testdrive:
        return Testdrive("")

    def manipulate(self) -> List[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                > CREATE USER create_user1 SUPERUSER;
                """,
                """
                > CREATE USER create_user2 SUPERUSER;
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > SELECT name FROM mz_roles WHERE name LIKE 'create_user%';
                create_user1
                create_user2
            """
            )
        )


class DropUser(Check):
    def initialize(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > CREATE USER drop_user1 SUPERUSER;
            """
            )
        )

    def manipulate(self) -> List[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                > DROP USER drop_user1;
                > CREATE USER drop_user2 SUPERUSER;
                """,
                """
                > DROP USER drop_user2;
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > SELECT COUNT(*) FROM mz_roles WHERE name LIKE 'drop_user%';
                0
            """
            )
        )
