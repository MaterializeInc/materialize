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
from materialize.checks.checks import Check


class PasswordAuth(Check):
    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                # Create a user with a password `user1` and grant them privileges to select from table materialize.schema1.t1
                """
                > DROP SCHEMA IF EXISTS schema1 CASCADE;
                > CREATE SCHEMA schema1;
                > SET SCHEMA = schema1;
                > CREATE ROLE user1 WITH LOGIN PASSWORD 'password';
                > CREATE TABLE t1 (c int);
                > GRANT USAGE ON SCHEMA schema1 TO user1;
                > GRANT SELECT ON TABLE t1 TO user1;

                # Validate that the user can select from the table
                $ postgres-execute connection=postgres://user1:password@${testdrive.materialize-password-sql-addr}
                SELECT * FROM materialize.schema1.t1
                """,
                # Change the role's password to `password2`
                """
                > ALTER ROLE user1 PASSWORD 'password2';
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            # Test that user1 can login and select from the table they have privileges to select from
            dedent(
                """
                $ postgres-execute connection=postgres://user1:password2@${testdrive.materialize-password-sql-addr}
                SELECT * FROM materialize.schema1.t1
                """
            )
        )
