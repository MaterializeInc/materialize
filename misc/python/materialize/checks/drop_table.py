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

    def manipulate(self) -> List[Testdrive]:
        fix_ownership = (
            """
                # When upgrading from old version without roles the table is
                # owned by default_role, thus we have to change the owner
                # before dropping it:
                $ postgres-execute connection=postgres://mz_system:materialize@materialized:6877
                ALTER TABLE drop_table2 OWNER TO materialize;
                """
            if self.base_version >= MzVersion.parse("0.47.0")
            else ""
        )

        return [
            Testdrive(dedent(s))
            for s in [
                """
                > DROP TABLE drop_table1;
                """,
                fix_ownership
                + """
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
