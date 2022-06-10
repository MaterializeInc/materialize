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
        return [
            Testdrive(dedent(s))
            for s in [
                """
                > DROP TABLE drop_table1;
                """,
                """
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
