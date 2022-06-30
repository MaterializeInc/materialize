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


class CreateIndex(Check):
    def initialize(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > CREATE TABLE create_index_table (f1 INTEGER, f2 INTEGER, f3 INTEGER);
                > INSERT INTO create_index_table VALUES (1,2,3);
                """
            )
        )

    def manipulate(self) -> List[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                > INSERT INTO create_index_table VALUES (2,3,4);
                > CREATE DEFAULT INDEX ON create_index_table;
                > INSERT INTO create_index_table VALUES (3,4,5);
                """,
                """
                > INSERT INTO create_index_table VALUES (4,5,6);
                > CREATE INDEX create_index_table_index ON create_index_table (f1, f2);
                > INSERT INTO create_index_table VALUES (5,6,7);
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > SELECT * FROM create_index_table;
                1 2 3
                2 3 4
                3 4 5
                4 5 6
                5 6 7

                > SELECT f1 FROM create_index_table;
                1
                2
                3
                4
                5

                > SELECT f3 FROM create_index_table;
                3
                4
                5
                6
                7
           """
            )
        )
