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


class DropTable(Check):
    def initialize(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > CREATE TABLE drop_table1 (f1 INTEGER);
                > INSERT INTO drop_table1 VALUES (1);

                > CREATE TABLE drop_table2 (f1 INTEGER);
                > INSERT INTO drop_table2 VALUES (1);
                """
            )
        )

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                > DROP TABLE drop_table1;
                """,
                """
                # When upgrading from old version without roles the table is
                # owned by default_role, thus we have to change the owner
                # before dropping it:
                $[version>=4700] postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
                ALTER TABLE drop_table2 OWNER TO materialize;

                > DROP TABLE drop_table2;
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                ! SELECT * FROM drop_table1;
                contains: unknown catalog item

                ! SELECT * FROM drop_table2;
                contains: unknown catalog item
           """
            )
        )
