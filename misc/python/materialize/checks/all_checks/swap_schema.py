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


class SwapSchema(Check):
    def initialize(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
            > CREATE SCHEMA swap_me1;
            > CREATE SCHEMA swap_me2;
            > CREATE SCHEMA swap_me3;
            > CREATE SCHEMA swap_me4;

            > CREATE TABLE swap_me1.t1 (f1 INTEGER);
            > CREATE TABLE swap_me2.t2 (f1 INTEGER);
            > CREATE TABLE swap_me3.t3 (f1 INTEGER);
            > CREATE TABLE swap_me4.t4 (f1 INTEGER);

            > INSERT INTO swap_me1.t1 VALUES (1);
            > INSERT INTO swap_me2.t2 VALUES (2);
            > INSERT INTO swap_me3.t3 VALUES (3);
            > INSERT INTO swap_me4.t4 VALUES (4);
            """
            )
        )

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                > ALTER SCHEMA swap_me1 SWAP WITH swap_me2;
                """,
                """
                > ALTER SCHEMA swap_me3 SWAP WITH swap_me4;
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > SHOW SCHEMAS LIKE 'swap_me%';
                swap_me1 ""
                swap_me2 ""
                swap_me3 ""
                swap_me4 ""

                > SET SCHEMA = swap_me1;

                > SELECT * FROM t2;
                2

                > SET SCHEMA = swap_me2;

                > SELECT * FROM t1;
                1

                > SET SCHEMA = swap_me3;

                > SELECT * FROM t4;
                4

                > SET SCHEMA = swap_me4;

                > SELECT * FROM t3;
                3
                """
            )
        )
