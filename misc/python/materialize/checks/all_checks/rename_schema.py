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


class RenameSchema(Check):
    def initialize(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
            > CREATE SCHEMA rename_me1;
            > CREATE SCHEMA rename_me2;
            > CREATE SCHEMA rename_me3;

            > CREATE TABLE rename_me1.t1 (f1 INTEGER);
            > CREATE TABLE rename_me2.t2 (f1 INTEGER);
            > CREATE TABLE rename_me3.t3 (f1 INTEGER);

            > INSERT INTO rename_me1.t1 VALUES (1);
            > INSERT INTO rename_me2.t2 VALUES (2);
            > INSERT INTO rename_me3.t3 VALUES (3);

            > ALTER SCHEMA rename_me1 RENAME TO renamed1;
            """
            )
        )

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                > ALTER SCHEMA rename_me2 RENAME TO renamed2;
                """,
                """
                > ALTER SCHEMA rename_me3 RENAME TO renamed3;
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > SHOW SCHEMAS LIKE 'rename%';
                renamed1 ""
                renamed2 ""
                renamed3 ""

                > SET SCHEMA = renamed1;

                > SELECT * FROM t1;
                1

                > SET SCHEMA = renamed2;

                > SELECT * FROM t2;
                2

                > SET SCHEMA = renamed3;

                > SELECT * FROM t3;
                3
                """
            )
        )
