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

    def _if_can_grant_revoke(self, text: str) -> str:
        if self.base_version >= MzVersion.parse("0.47.0-dev"):
            return text
        return ""

    def initialize(self) -> Testdrive:
        return Testdrive("")

    def manipulate(self) -> List[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                > CREATE ROLE create_role1;
                """
                + self._if_can_grant_revoke(
                    """
                > GRANT create_role1 TO materialize;
                """
                ),
                """
                > CREATE ROLE create_role2;
                """
                + self._if_can_grant_revoke(
                    """
                > GRANT create_role2 TO materialize;
                """
                ),
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
                + self._if_can_grant_revoke(
                    """
                > SELECT role.name, member.name, grantor.name from mz_role_members JOIN mz_roles role ON mz_role_members.role_id = role.id JOIN mz_roles member ON mz_role_members.member = member.id JOIN mz_roles grantor ON mz_role_members.grantor = grantor.id WHERE role.name LIKE 'create_role%';
                create_role1 materialize materialize
                create_role2 materialize materialize
                """
                )
            )
        )


class DropRole(CreateRole):
    def initialize(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > CREATE ROLE drop_role1;
                """
                + self._if_can_grant_revoke(
                    """
                > GRANT drop_role1 TO materialize;
                """
                )
            )
        )

    def manipulate(self) -> List[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                self._if_can_grant_revoke(
                    """
                > REVOKE drop_role1 FROM materialize;
                """
                )
                + """
                > DROP ROLE drop_role1;
                > CREATE ROLE drop_role2;
                """
                + self._if_can_grant_revoke(
                    """
                > GRANT drop_role2 TO materialize;
                """
                ),
                self._if_can_grant_revoke(
                    """
                > REVOKE drop_role2 FROM materialize;
                """
                )
                + """
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
                + self._if_can_grant_revoke(
                    """
                > SELECT COUNT(*) FROM mz_role_members JOIN mz_roles ON mz_role_members.role_id = mz_roles.id WHERE name LIKE 'drop_role%';
                0
                """
                )
            )
        )
