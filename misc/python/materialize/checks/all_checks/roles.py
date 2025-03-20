# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from textwrap import dedent

from materialize.checks.actions import Testdrive
from materialize.checks.checks import TESTDRIVE_NOP, Check


class CreateRole(Check):
    def initialize(self) -> Testdrive:
        return Testdrive(TESTDRIVE_NOP)

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                > CREATE ROLE create_role1;
                > GRANT create_role1 TO materialize;
                """,
                """
                > CREATE ROLE create_role2;
                > GRANT create_role2 TO materialize;
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
                # TODO(def-) Grantor information is currently not stable during
                # upgrades due to https://github.com/MaterializeInc/materialize/pull/18780
                # Reenable on next release
                > SELECT role.name, member.name from mz_role_members JOIN mz_roles role ON mz_role_members.role_id = role.id JOIN mz_roles member ON mz_role_members.member = member.id JOIN mz_roles grantor ON mz_role_members.grantor = grantor.id WHERE role.name LIKE 'create_role%';
                create_role1 materialize
                create_role2 materialize
                """
            )
        )


class DropRole(CreateRole):
    def initialize(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > CREATE ROLE drop_role1;
                > GRANT drop_role1 TO materialize;
                """
            )
        )

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                > REVOKE drop_role1 FROM materialize;
                > DROP ROLE drop_role1;
                > CREATE ROLE drop_role2;
                > GRANT drop_role2 TO materialize;
                """,
                """
                > REVOKE drop_role2 FROM materialize;
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
                > SELECT COUNT(*) FROM mz_role_members JOIN mz_roles ON mz_role_members.role_id = mz_roles.id WHERE name LIKE 'drop_role%';
                0
                """
            )
        )


class BuiltinRoles(CreateRole):
    def manipulate(self) -> list[Testdrive]:
        return [Testdrive(TESTDRIVE_NOP), Testdrive(TESTDRIVE_NOP)]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
            $ skip-if
            SELECT mz_version_num() < 8300

            > SELECT name FROM mz_roles WHERE name IN ('mz_monitor', 'mz_monitor_redacted') ORDER BY name
            mz_monitor
            mz_monitor_redacted
            """
            )
        )
